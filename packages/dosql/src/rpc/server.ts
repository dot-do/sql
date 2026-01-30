/**
 * DoSQL RPC Server
 *
 * Server implementation for handling CapnWeb RPC requests in Durable Objects.
 * Provides the RpcTarget implementation and request handlers.
 *
 * @packageDocumentation
 */

import {
  RpcTarget,
  newWorkersRpcResponse,
  newHttpBatchRpcResponse,
  newWorkersWebSocketRpcResponse,
  type RpcSessionOptions,
} from 'capnweb';

import { TIMEOUTS, SIZE_LIMITS, CDC, STREAMS } from '../constants.js';

import type {
  DoSQLAPI,
  QueryRequest,
  QueryResponse,
  StreamRequest,
  StreamChunk,
  CDCRequest,
  CDCEvent,
  CDCAck,
  BeginTransactionRequest,
  TransactionHandle,
  CommitRequest,
  RollbackRequest,
  TransactionResult,
  BatchRequest,
  BatchResponse,
  SchemaRequest,
  SchemaResponse,
  ConnectionStats,
  RPCError,
  RPCErrorCode,
  ColumnType,
  TableSchema,
} from './types.js';

import { parseParameters } from '../statement/binding.js';
import { createMissingNamedParamError } from '../errors/index.js';

// =============================================================================
// Server Types
// =============================================================================

/**
 * Query executor interface
 *
 * Implement this interface to connect the RPC server to your SQL engine.
 */
export interface QueryExecutor {
  /** Execute a SQL query */
  execute(sql: string, params?: unknown[], options?: ExecuteOptions): Promise<ExecuteResult>;
  /** Get current LSN */
  getCurrentLSN(): bigint;
  /** Get schema information */
  getSchema(tables?: string[]): Promise<TableSchema[]>;
  /** Begin a transaction */
  beginTransaction(options?: TransactionOptions): Promise<string>;
  /** Commit a transaction */
  commit(txId: string): Promise<void>;
  /** Rollback a transaction */
  rollback(txId: string, savepoint?: string): Promise<void>;
}

export interface ExecuteOptions {
  /** Branch/namespace */
  branch?: string;
  /** Time travel LSN */
  asOf?: bigint;
  /** Query timeout */
  timeoutMs?: number;
  /** Transaction ID */
  txId?: string;
  /** Max rows to return */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
}

export interface ExecuteResult {
  /** Column names */
  columns: string[];
  /** Column types */
  columnTypes: ColumnType[];
  /** Result rows */
  rows: unknown[][];
  /** Row count */
  rowCount: number;
  /** Resulting LSN */
  lsn: bigint;
}

export interface TransactionOptions {
  isolation?: 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE';
  readOnly?: boolean;
  timeoutMs?: number;
}

/**
 * CDC manager interface for handling change subscriptions
 */
export interface CDCManager {
  /** Subscribe to changes */
  subscribe(options: CDCSubscribeOptions): CDCSubscription;
  /** Get events since LSN */
  getEventsSince(fromLSN: bigint, tables?: string[]): Promise<CDCEvent[]>;
}

export interface CDCSubscribeOptions {
  fromLSN: bigint;
  tables?: string[];
  operations?: string[];
  includeRowData?: boolean;
}

export interface CDCSubscription {
  id: string;
  /** Async iterator for events */
  events: AsyncIterable<CDCEvent>;
  /** Unsubscribe */
  unsubscribe(): void;
}

// =============================================================================
// DoSQL RpcTarget Implementation
// =============================================================================

/**
 * DoSQLTarget - CapnWeb RpcTarget for DoSQL
 *
 * This class extends RpcTarget to expose DoSQL methods via CapnWeb RPC.
 * It wraps a QueryExecutor to handle actual SQL execution.
 *
 * @example
 * ```typescript
 * // In your Durable Object
 * export class DoSQLDurableObject implements DurableObject {
 *   private target: DoSQLTarget;
 *
 *   constructor(ctx: DurableObjectState, env: Env) {
 *     const executor = new MyQueryExecutor(ctx);
 *     this.target = new DoSQLTarget(executor);
 *   }
 *
 *   async fetch(request: Request): Promise<Response> {
 *     return handleDoSQLRequest(request, this.target);
 *   }
 * }
 * ```
 */
export class DoSQLTarget extends RpcTarget implements DoSQLAPI {
  #executor: QueryExecutor;
  #cdcManager?: CDCManager;
  #connections: Map<string, ConnectionInfo> = new Map();
  #streams: Map<string, StreamState> = new Map();
  #cdcSubscriptions: Map<string, CDCSubscription> = new Map();
  #stats: ServerStats = {
    totalQueries: 0,
    totalTransactions: 0,
    activeConnections: 0,
    activeTransactions: 0,
  };

  // Stream cleanup options and tracking
  #streamTTLMs: number;
  #cdcStreamTTLMs?: number;
  #queryStreamTTLMs?: number;
  #maxConcurrentStreams: number;
  #maxQueryStreams?: number;
  #maxCdcSubscriptions?: number;
  #onScheduleAlarm?: (delayMs: number) => void;
  #alarmPending = false;
  #streamStats: StreamStats = {
    activeStreams: 0,
    totalCreated: 0,
    totalClosed: 0,
  };
  #cleanupStats: CleanupStats = {
    manualCloses: 0,
    ttlExpiries: 0,
    connectionCloses: 0,
    abandonedCleanups: 0,
    memoryPressureCleanups: 0,
  };
  #cleanupCallbacks: Array<() => void> = [];
  #memoryWarningThreshold: number;
  #memoryCriticalThreshold: number;
  #peakMemoryUsage = 0;
  #memoryMetricsCallbacks: Array<(metrics: MemoryMetrics) => void> = [];

  constructor(executor: QueryExecutor, cdcManager?: CDCManager, options?: DoSQLTargetOptions) {
    super();
    this.#executor = executor;
    this.#cdcManager = cdcManager;
    this.#streamTTLMs = options?.streamTTLMs ?? TIMEOUTS.DEFAULT_STREAM_TTL_MS;
    this.#maxConcurrentStreams = options?.maxConcurrentStreams ?? STREAMS.MAX_CONCURRENT_STREAMS;
    this.#onScheduleAlarm = options?.onScheduleAlarm;
    this.#memoryWarningThreshold = options?.memoryWarningThreshold ?? 50 * 1024 * 1024; // 50MB
    this.#memoryCriticalThreshold = options?.memoryCriticalThreshold ?? 100 * 1024 * 1024; // 100MB
  }

  // ===========================================================================
  // Query Operations
  // ===========================================================================

  async query(request: QueryRequest): Promise<QueryResponse> {
    const startTime = performance.now();
    this.#stats.totalQueries++;

    try {
      const result = await this.#executor.execute(
        request.sql,
        request.params ?? request.namedParams ? this.#convertNamedParams(request) : undefined,
        {
          branch: request.branch,
          asOf: request.asOf,
          timeoutMs: request.timeoutMs,
          limit: request.limit,
          offset: request.offset,
        }
      );

      return {
        columns: result.columns,
        columnTypes: result.columnTypes,
        rows: result.rows,
        rowCount: result.rowCount,
        lsn: result.lsn,
        executionTimeMs: performance.now() - startTime,
        hasMore: request.limit ? result.rowCount >= request.limit : false,
      };
    } catch (error) {
      throw this.#wrapError(error);
    }
  }

  // Streaming is handled via internal methods called by the client
  // The actual async iteration happens client-side

  async _initStream(request: StreamRequest & { streamId: string }): Promise<{
    columns: string[];
    columnTypes: ColumnType[];
  }> {
    // Check memory limits
    const memUsage = this.getMemoryUsage();
    if (memUsage.status === 'critical') {
      this.#cleanupStats.memoryPressureCleanups++;
      throw new Error(`Critical memory limit reached (${memUsage.estimatedBytes} bytes, limit: ${this.#memoryCriticalThreshold})`);
    }

    // Auto-cleanup expired streams before enforcing limit
    if (this.#streams.size >= this.#maxConcurrentStreams) {
      this.cleanupExpiredStreams();
    }

    // Check maximum concurrent streams limit (after cleanup)
    if (this.#streams.size >= this.#maxConcurrentStreams) {
      throw new Error(`Maximum concurrent streams exceeded (limit: ${this.#maxConcurrentStreams})`);
    }

    // Initialize stream state
    const result = await this.#executor.execute(
      request.sql,
      request.params,
      { branch: request.branch, limit: 1 } // Get just schema info
    );

    const now = Date.now();

    this.#streams.set(request.streamId, {
      sql: request.sql,
      params: request.params,
      branch: request.branch,
      chunkSize: request.chunkSize ?? SIZE_LIMITS.DEFAULT_CHUNK_SIZE,
      maxRows: request.maxRows,
      offset: 0,
      totalRowsSent: 0,
      columns: result.columns,
      columnTypes: result.columnTypes,
      createdAt: now,
      lastActivity: now,
    });

    // Update stats
    this.#streamStats.activeStreams++;
    this.#streamStats.totalCreated++;

    // Schedule cleanup alarm if this is the first stream
    this.#scheduleCleanupAlarm();

    return {
      columns: result.columns,
      columnTypes: result.columnTypes,
    };
  }

  async _nextChunk(streamId: string): Promise<StreamChunk> {
    const stream = this.#streams.get(streamId);
    if (!stream) {
      throw new Error(`Stream ${streamId} not found`);
    }

    // Update last activity timestamp
    stream.lastActivity = Date.now();

    const result = await this.#executor.execute(
      stream.sql,
      stream.params,
      {
        branch: stream.branch,
        limit: stream.chunkSize,
        offset: stream.offset,
      }
    );

    stream.offset += result.rowCount;
    stream.totalRowsSent += result.rowCount;

    const isLast = result.rowCount < stream.chunkSize ||
      (stream.maxRows !== undefined && stream.totalRowsSent >= stream.maxRows);

    if (isLast) {
      this.#streams.delete(streamId);
      this.#streamStats.activeStreams--;
      this.#streamStats.totalClosed++;
    }

    return {
      chunkIndex: Math.floor((stream.offset - result.rowCount) / stream.chunkSize),
      rows: result.rows,
      rowCount: result.rowCount,
      isLast,
      totalRowsSoFar: stream.totalRowsSent,
    };
  }

  async _closeStream(streamId: string): Promise<void> {
    this.#streams.delete(streamId);
  }

  // queryStream is not directly callable - it returns an async iterable
  // which doesn't serialize over RPC. Instead, clients use _initStream/_nextChunk
  queryStream(_request: StreamRequest): AsyncIterable<StreamChunk> {
    throw new Error('Use _initStream and _nextChunk for streaming');
  }

  // ===========================================================================
  // Transaction Operations
  // ===========================================================================

  async beginTransaction(request: BeginTransactionRequest): Promise<TransactionHandle> {
    this.#stats.totalTransactions++;
    this.#stats.activeTransactions++;

    try {
      const txId = await this.#executor.beginTransaction({
        isolation: request.isolation,
        readOnly: request.readOnly,
        timeoutMs: request.timeoutMs,
      });

      const expiresAt = Date.now() + (request.timeoutMs ?? TIMEOUTS.DEFAULT_TRANSACTION_MS);

      return {
        txId,
        startLSN: this.#executor.getCurrentLSN(),
        expiresAt,
      };
    } catch (error) {
      this.#stats.activeTransactions--;
      throw this.#wrapError(error);
    }
  }

  async commit(request: CommitRequest): Promise<TransactionResult> {
    try {
      await this.#executor.commit(request.txId);
      this.#stats.activeTransactions--;

      return {
        success: true,
        lsn: this.#executor.getCurrentLSN(),
      };
    } catch (error) {
      this.#stats.activeTransactions--;
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async rollback(request: RollbackRequest): Promise<TransactionResult> {
    try {
      await this.#executor.rollback(request.txId, request.savepoint);
      if (!request.savepoint) {
        this.#stats.activeTransactions--;
      }

      return { success: true };
    } catch (error) {
      if (!request.savepoint) {
        this.#stats.activeTransactions--;
      }
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  // ===========================================================================
  // Batch Operations
  // ===========================================================================

  async batch(request: BatchRequest): Promise<BatchResponse> {
    const startTime = performance.now();
    const results: Array<QueryResponse | { index: number; error: string; code?: string }> = [];
    let successCount = 0;
    let errorCount = 0;

    // If atomic, wrap in transaction
    let txId: string | undefined;
    if (request.atomic) {
      txId = await this.#executor.beginTransaction({});
    }

    try {
      for (let i = 0; i < request.queries.length; i++) {
        const queryRequest = request.queries[i];

        try {
          const result = await this.query({
            ...queryRequest,
            branch: queryRequest.branch ?? request.branch,
          });
          results.push(result);
          successCount++;
        } catch (error) {
          errorCount++;
          results.push({
            index: i,
            error: error instanceof Error ? error.message : String(error),
            code: this.#getErrorCode(error),
          });

          if (!request.continueOnError) {
            // Add placeholders for remaining queries
            for (let j = i + 1; j < request.queries.length; j++) {
              results.push({
                index: j,
                error: 'Skipped due to previous error',
                code: 'SKIPPED',
              });
            }
            break;
          }
        }
      }

      // Commit if atomic and all succeeded
      if (txId) {
        if (errorCount === 0) {
          await this.#executor.commit(txId);
        } else {
          await this.#executor.rollback(txId);
        }
      }
    } catch (error) {
      // Rollback on any error
      if (txId) {
        await this.#executor.rollback(txId).catch(() => {});
      }
      throw this.#wrapError(error);
    }

    return {
      results,
      successCount,
      errorCount,
      executionTimeMs: performance.now() - startTime,
      lsn: this.#executor.getCurrentLSN(),
    };
  }

  // ===========================================================================
  // CDC Operations
  // ===========================================================================

  async _subscribeCDC(request: CDCRequest & { subscriptionId: string }): Promise<CDCAck> {
    if (!this.#cdcManager) {
      throw new Error('CDC not supported');
    }

    const subscription = this.#cdcManager.subscribe({
      fromLSN: request.fromLSN,
      tables: request.tables,
      operations: request.operations,
      includeRowData: request.includeRowData,
    });

    this.#cdcSubscriptions.set(request.subscriptionId, subscription);

    return {
      subscriptionId: request.subscriptionId,
      currentLSN: this.#executor.getCurrentLSN(),
      subscribedTables: request.tables ?? ['*'],
    };
  }

  async _pollCDC(subscriptionId: string): Promise<CDCEvent[]> {
    const subscription = this.#cdcSubscriptions.get(subscriptionId);
    if (!subscription) {
      throw new Error(`Subscription ${subscriptionId} not found`);
    }

    // Collect available events (non-blocking)
    const events: CDCEvent[] = [];
    const iterator = subscription.events[Symbol.asyncIterator]();

    // Poll with timeout
    const timeoutPromise = new Promise<{ done: true }>((resolve) =>
      setTimeout(() => resolve({ done: true }), CDC.POLL_TIMEOUT_MS)
    );

    while (events.length < CDC.MAX_EVENTS_PER_POLL) {
      const result = await Promise.race([
        iterator.next(),
        timeoutPromise,
      ]);

      if (result.done) break;
      events.push(result.value as CDCEvent);
    }

    return events;
  }

  async _unsubscribeCDC(subscriptionId: string): Promise<void> {
    const subscription = this.#cdcSubscriptions.get(subscriptionId);
    if (subscription) {
      subscription.unsubscribe();
      this.#cdcSubscriptions.delete(subscriptionId);
    }
  }

  // Public CDC methods that throw (actual implementation via internal methods)
  subscribeCDC(_request: CDCRequest): AsyncIterable<CDCEvent> {
    throw new Error('Use _subscribeCDC and _pollCDC for CDC subscriptions');
  }

  async unsubscribeCDC(subscriptionId: string): Promise<void> {
    return this._unsubscribeCDC(subscriptionId);
  }

  // ===========================================================================
  // Schema Operations
  // ===========================================================================

  async getSchema(request: SchemaRequest): Promise<SchemaResponse> {
    const tables = await this.#executor.getSchema(request.tables);

    return {
      tables,
      version: 1, // Schema version tracking would be implementation-specific
      lastModifiedLSN: this.#executor.getCurrentLSN(),
    };
  }

  // ===========================================================================
  // Connection Operations
  // ===========================================================================

  async ping(): Promise<{ pong: true; lsn: bigint; timestamp: number }> {
    return {
      pong: true,
      lsn: this.#executor.getCurrentLSN(),
      timestamp: Date.now(),
    };
  }

  async getStats(): Promise<ConnectionStats> {
    return {
      connected: true,
      currentLSN: this.#executor.getCurrentLSN(),
      messagesSent: 0,
      messagesReceived: 0,
      reconnectCount: 0,
    };
  }

  // ===========================================================================
  // Server Statistics
  // ===========================================================================

  getServerStats(): ServerStats {
    return { ...this.#stats };
  }

  // ===========================================================================
  // Stream Management API
  // ===========================================================================

  /**
   * Get stream statistics
   */
  getStreamStats(): StreamStats {
    return {
      activeStreams: this.#streams.size,
      totalCreated: this.#streamStats.totalCreated,
      totalClosed: this.#streamStats.totalClosed,
    };
  }

  /**
   * Get information about a specific stream
   */
  getStreamInfo(streamId: string): StreamInfo | undefined {
    const stream = this.#streams.get(streamId);
    if (!stream) return undefined;

    return {
      streamId,
      sql: stream.sql,
      createdAt: stream.createdAt,
      lastActivity: stream.lastActivity,
      chunkSize: stream.chunkSize,
      totalRowsSent: stream.totalRowsSent,
    };
  }

  /**
   * Get maximum concurrent streams limit
   */
  getMaxConcurrentStreams(): number {
    return this.#maxConcurrentStreams;
  }

  /**
   * Close a specific stream
   * @returns true if stream was closed, false if stream didn't exist
   */
  closeStream(streamId: string): boolean {
    const existed = this.#streams.delete(streamId);
    if (existed) {
      this.#streamStats.activeStreams--;
      this.#streamStats.totalClosed++;
      this.#cleanupStats.manualCloses++;
    }
    return existed;
  }

  /**
   * Close all streams
   * @returns number of streams that were closed
   */
  closeAllStreams(): number {
    const count = this.#streams.size;
    this.#streams.clear();
    this.#streamStats.activeStreams = 0;
    this.#streamStats.totalClosed += count;
    return count;
  }

  /**
   * Cleanup expired streams based on TTL
   */
  cleanupExpiredStreams(): number {
    const now = Date.now();
    const expiredStreamIds: string[] = [];

    for (const [streamId, stream] of this.#streams) {
      // Use per-stream-type TTL if configured
      const ttl = stream.streamType === 'cdc'
        ? (this.#cdcStreamTTLMs ?? this.#streamTTLMs)
        : (this.#queryStreamTTLMs ?? this.#streamTTLMs);
      if (now - stream.lastActivity > ttl) {
        expiredStreamIds.push(streamId);
      }
    }

    for (const streamId of expiredStreamIds) {
      this.#streams.delete(streamId);
      this.#streamStats.activeStreams--;
      this.#streamStats.totalClosed++;
    }
    this.#cleanupStats.ttlExpiries += expiredStreamIds.length;

    // Reschedule alarm if there are still active streams
    if (this.#streams.size > 0) {
      this.#alarmPending = false;
      this.#scheduleCleanupAlarm();
    } else {
      this.#alarmPending = false;
    }

    return expiredStreamIds.length;
  }

  /**
   * Called when WebSocket connection closes to cleanup all resources
   */
  onConnectionClose(): void {
    const streamCount = this.#streams.size;
    // Close all streams
    this.closeAllStreams();
    this.#cleanupStats.connectionCloses += streamCount;
    // Undo the manual close tracking from closeAllStreams (those were connection closes, not manual)
    // closeAllStreams doesn't add to manualCloses, so no adjustment needed

    // Close all CDC subscriptions
    for (const [, subscription] of this.#cdcSubscriptions) {
      subscription.unsubscribe();
    }
    this.#cdcSubscriptions.clear();

    // Call registered cleanup callbacks
    for (const callback of this.#cleanupCallbacks) {
      try {
        callback();
      } catch {
        // Handle cleanup errors gracefully - don't let one callback failure
        // prevent other callbacks from running
      }
    }
  }

  // ===========================================================================
  // Stream Manager API
  // ===========================================================================

  /**
   * Get a stream manager interface for advanced stream management
   */
  getStreamManager(): StreamManager {
    return {
      setMaxConcurrentStreams: (limit: number) => {
        this.#maxConcurrentStreams = limit;
      },
      setStreamTTL: (ttlMs: number) => {
        this.#streamTTLMs = ttlMs;
      },
      setStreamTTLByType: (type: 'query' | 'cdc', ttlMs: number) => {
        if (type === 'cdc') {
          this.#cdcStreamTTLMs = ttlMs;
        } else {
          this.#queryStreamTTLMs = ttlMs;
        }
      },
      setStreamLimits: (options: { maxQueryStreams?: number; maxCdcSubscriptions?: number }) => {
        this.#maxQueryStreams = options.maxQueryStreams;
        this.#maxCdcSubscriptions = options.maxCdcSubscriptions;
      },
      getStreamCount: () => this.#streams.size,
      getStreamStats: () => ({
        ...this.getStreamStats(),
        totalCdcSubscriptions: this.#cdcSubscriptions.size,
      }),
      getCleanupStats: () => ({ ...this.#cleanupStats }),
    };
  }

  /**
   * Register a cleanup callback to be called on connection close
   */
  registerCleanupCallback(callback: () => void): void {
    this.#cleanupCallbacks.push(callback);
  }

  /**
   * Close streams whose IDs match a pattern (glob-like with * wildcard)
   */
  closeStreamsByPattern(pattern: string): number {
    const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');
    const toClose: string[] = [];
    for (const streamId of this.#streams.keys()) {
      if (regex.test(streamId)) {
        toClose.push(streamId);
      }
    }
    for (const streamId of toClose) {
      this.closeStream(streamId);
    }
    return toClose.length;
  }

  /**
   * Get list of abandoned streams (streams that have been inactive longer than TTL)
   */
  getAbandonedStreams(thresholdMs?: number): StreamInfo[] {
    const threshold = thresholdMs ?? this.#streamTTLMs;
    const now = Date.now();
    const abandoned: StreamInfo[] = [];
    for (const [streamId, stream] of this.#streams) {
      if (now - stream.lastActivity > threshold) {
        abandoned.push({
          streamId,
          sql: stream.sql,
          createdAt: stream.createdAt,
          lastActivity: stream.lastActivity,
          chunkSize: stream.chunkSize,
          totalRowsSent: stream.totalRowsSent,
        });
      }
    }
    return abandoned;
  }

  /**
   * Get estimated memory usage for all streams
   */
  getMemoryUsage(): MemoryUsage {
    let estimatedBytes = 0;
    for (const [, stream] of this.#streams) {
      // Estimate: base overhead + SQL string + per-row estimate
      estimatedBytes += 200; // base overhead per stream
      estimatedBytes += (stream.sql.length * 2); // SQL string (2 bytes per char)
      estimatedBytes += (stream.columns.length * 50); // column metadata
      estimatedBytes += (stream.totalRowsSent * 100); // rough per-row buffered estimate
    }

    // Track peak
    if (estimatedBytes > this.#peakMemoryUsage) {
      this.#peakMemoryUsage = estimatedBytes;
    }

    const status: 'ok' | 'warning' | 'critical' =
      estimatedBytes >= this.#memoryCriticalThreshold ? 'critical' :
      estimatedBytes >= this.#memoryWarningThreshold ? 'warning' : 'ok';

    return {
      estimatedBytes,
      streamCount: this.#streams.size,
      cdcSubscriptionCount: this.#cdcSubscriptions.size,
      warningThreshold: this.#memoryWarningThreshold,
      criticalThreshold: this.#memoryCriticalThreshold,
      status,
    };
  }

  /**
   * Get memory usage breakdown by stream type
   */
  getMemoryBreakdown(): { query: number; cdc: number; total: number } {
    let queryBytes = 0;
    let cdcBytes = 0;
    for (const [, stream] of this.#streams) {
      const bytes = 200 + (stream.sql.length * 2) + (stream.columns.length * 50);
      if (stream.streamType === 'cdc') {
        cdcBytes += bytes;
      } else {
        queryBytes += bytes;
      }
    }
    return { query: queryBytes, cdc: cdcBytes, total: queryBytes + cdcBytes };
  }

  /**
   * Get peak memory usage observed
   */
  getPeakMemoryUsage(): number {
    return this.#peakMemoryUsage;
  }

  /**
   * Register a callback for memory metrics emission
   */
  onMemoryMetrics(callback: (metrics: MemoryMetrics) => void): void {
    this.#memoryMetricsCallbacks.push(callback);
  }

  /**
   * Emit current memory metrics to registered callbacks
   */
  emitMemoryMetrics(): void {
    const usage = this.getMemoryUsage();
    const metrics: MemoryMetrics = {
      estimatedBytes: usage.estimatedBytes,
      streamCount: usage.streamCount,
      cdcSubscriptionCount: usage.cdcSubscriptionCount,
      status: usage.status,
      peakBytes: this.#peakMemoryUsage,
      timestamp: Date.now(),
    };
    for (const callback of this.#memoryMetricsCallbacks) {
      callback(metrics);
    }
  }

  /**
   * Graceful shutdown - cleanup all streams and CDC subscriptions
   */
  async gracefulShutdown(_timeoutMs: number): Promise<void> {
    // Close all streams
    this.closeAllStreams();

    // Close all CDC subscriptions
    for (const [, subscription] of this.#cdcSubscriptions) {
      subscription.unsubscribe();
    }
    this.#cdcSubscriptions.clear();

    // Call registered cleanup callbacks
    for (const callback of this.#cleanupCallbacks) {
      try {
        callback();
      } catch {
        // Graceful - don't let callback errors prevent shutdown
      }
    }
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  /**
   * Schedule cleanup alarm if not already pending
   */
  #scheduleCleanupAlarm(): void {
    if (this.#alarmPending || !this.#onScheduleAlarm) return;
    if (this.#streams.size === 0) return;

    this.#alarmPending = true;
    // Schedule alarm for TTL + 1 minute buffer
    this.#onScheduleAlarm(this.#streamTTLMs + TIMEOUTS.STREAM_CLEANUP_BUFFER_MS);
  }

  #convertNamedParams(request: QueryRequest): unknown[] {
    // Convert named parameters to positional based on proper SQL parsing
    // Uses parseParameters from binding.ts which respects string literals and comments
    if (!request.namedParams) return request.params ?? [];

    const namedParams = request.namedParams;

    // Parse the SQL to extract parameter tokens (respects string boundaries and comments)
    const parsed = parseParameters(request.sql);

    // If no named parameters in SQL, return positional params if provided
    if (!parsed.hasNamedParameters) {
      return request.params ?? [];
    }

    // Build positional params array from named params based on parsed tokens
    const params: unknown[] = [];
    for (const token of parsed.tokens) {
      if (token.type === 'named') {
        const paramName = token.key as string;
        if (!(paramName in namedParams)) {
          throw createMissingNamedParamError(paramName);
        }
        params.push(namedParams[paramName]);
      } else {
        // For positional/numbered params, use from request.params if available
        const index = (token.key as number) - 1;
        if (request.params && index < request.params.length) {
          params.push(request.params[index]);
        }
      }
    }

    return params;
  }

  #wrapError(error: unknown): RPCError {
    if (error instanceof Error) {
      return {
        code: this.#getErrorCode(error) as RPCErrorCode,
        message: error.message,
        // Stack traces are omitted in production (Workers don't have process.env)
        stack: undefined,
      };
    }
    return {
      code: 'UNKNOWN' as RPCErrorCode,
      message: String(error),
    };
  }

  #getErrorCode(error: unknown): string {
    if (error instanceof Error) {
      // Map common error patterns to error codes
      const message = error.message.toLowerCase();
      if (message.includes('syntax')) return 'SYNTAX_ERROR';
      if (message.includes('table') && message.includes('not found')) return 'TABLE_NOT_FOUND';
      if (message.includes('column') && message.includes('not found')) return 'COLUMN_NOT_FOUND';
      if (message.includes('constraint')) return 'CONSTRAINT_VIOLATION';
      if (message.includes('timeout')) return 'TIMEOUT';
      if (message.includes('deadlock')) return 'DEADLOCK_DETECTED';
      if (message.includes('serialization')) return 'SERIALIZATION_FAILURE';
    }
    return 'UNKNOWN';
  }
}

// =============================================================================
// Stream State
// =============================================================================

interface StreamState {
  sql: string;
  params?: unknown[];
  branch?: string;
  chunkSize: number;
  maxRows?: number;
  offset: number;
  totalRowsSent: number;
  columns: string[];
  columnTypes: ColumnType[];
  /** Timestamp when stream was created */
  createdAt: number;
  /** Timestamp of last activity (creation or chunk retrieval) */
  lastActivity: number;
  /** Stream type for per-type TTL configuration */
  streamType?: 'query' | 'cdc';
}

/**
 * Public stream info returned by getStreamInfo()
 */
export interface StreamInfo {
  streamId: string;
  sql: string;
  createdAt: number;
  lastActivity: number;
  chunkSize: number;
  totalRowsSent: number;
}

/**
 * Stream statistics
 */
export interface StreamStats {
  /** Number of currently active streams */
  activeStreams: number;
  /** Total number of streams created */
  totalCreated: number;
  /** Total number of streams closed */
  totalClosed: number;
}

/**
 * Configuration options for DoSQLTarget
 */
export interface DoSQLTargetOptions {
  /** TTL for inactive streams in milliseconds (default: 30 minutes) */
  streamTTLMs?: number;
  /** Maximum concurrent streams per connection (default: 100) */
  maxConcurrentStreams?: number;
  /** Callback to schedule alarm for cleanup (for Durable Object integration) */
  onScheduleAlarm?: (delayMs: number) => void;
  /** Memory warning threshold in bytes (default: 50MB) */
  memoryWarningThreshold?: number;
  /** Memory critical threshold in bytes (default: 100MB) */
  memoryCriticalThreshold?: number;
}

/**
 * Cleanup activity statistics
 */
export interface CleanupStats {
  manualCloses: number;
  ttlExpiries: number;
  connectionCloses: number;
  abandonedCleanups: number;
  memoryPressureCleanups: number;
}

/**
 * Stream manager interface for advanced stream management
 */
export interface StreamManager {
  setMaxConcurrentStreams(limit: number): void;
  setStreamTTL(ttlMs: number): void;
  setStreamTTLByType(type: 'query' | 'cdc', ttlMs: number): void;
  setStreamLimits(options: { maxQueryStreams?: number; maxCdcSubscriptions?: number }): void;
  getStreamCount(): number;
  getStreamStats(): StreamStats & { totalCdcSubscriptions: number };
  getCleanupStats(): CleanupStats;
}

/**
 * Memory usage information
 */
export interface MemoryUsage {
  estimatedBytes: number;
  streamCount: number;
  cdcSubscriptionCount: number;
  warningThreshold: number;
  criticalThreshold: number;
  status: 'ok' | 'warning' | 'critical';
}

/**
 * Memory metrics for monitoring
 */
export interface MemoryMetrics {
  estimatedBytes: number;
  streamCount: number;
  cdcSubscriptionCount: number;
  status: 'ok' | 'warning' | 'critical';
  peakBytes: number;
  timestamp: number;
}

interface ConnectionInfo {
  id: string;
  connectedAt: number;
  lastActivity: number;
  branch?: string;
}

interface ServerStats {
  totalQueries: number;
  totalTransactions: number;
  activeConnections: number;
  activeTransactions: number;
}

// =============================================================================
// Request Handler
// =============================================================================

/**
 * Handle a DoSQL RPC request
 *
 * Automatically detects WebSocket upgrades and HTTP batch requests,
 * routing to the appropriate CapnWeb handler.
 *
 * @param request - Incoming HTTP request
 * @param target - DoSQLTarget instance
 * @param options - RPC session options
 * @returns HTTP response
 *
 * @example
 * ```typescript
 * export class DoSQLDurableObject implements DurableObject {
 *   private target: DoSQLTarget;
 *
 *   constructor(ctx: DurableObjectState, env: Env) {
 *     this.target = new DoSQLTarget(new MyExecutor(ctx));
 *   }
 *
 *   async fetch(request: Request): Promise<Response> {
 *     const url = new URL(request.url);
 *
 *     if (url.pathname === '/rpc' || url.pathname === '/api') {
 *       return handleDoSQLRequest(request, this.target);
 *     }
 *
 *     return new Response('Not Found', { status: 404 });
 *   }
 * }
 * ```
 */
export async function handleDoSQLRequest(
  request: Request,
  target: DoSQLTarget,
  options?: RpcSessionOptions
): Promise<Response> {
  const sessionOptions: RpcSessionOptions = {
    onSendError: (error: Error) => {
      console.error('[DoSQL RPC] Error:', error);
      // Redact stack traces in production
      const redactedError = new Error(error.message);
      redactedError.name = error.name;
      return redactedError;
    },
    ...options,
  };

  // WebSocket upgrade
  const upgradeHeader = request.headers.get('upgrade');
  if (upgradeHeader?.toLowerCase() === 'websocket') {
    return newWorkersWebSocketRpcResponse(request, target, sessionOptions);
  }

  // HTTP POST - batch mode
  if (request.method === 'POST') {
    return newHttpBatchRpcResponse(request, target, sessionOptions);
  }

  // Fallback (handles other cases)
  return newWorkersRpcResponse(request, target);
}

/**
 * Check if a request is a DoSQL RPC request
 */
export function isDoSQLRequest(request: Request): boolean {
  // POST requests (HTTP batch)
  if (request.method === 'POST') return true;

  // WebSocket upgrades
  const upgradeHeader = request.headers.get('upgrade');
  if (upgradeHeader?.toLowerCase() === 'websocket') return true;

  return false;
}

// =============================================================================
// Mock Query Executor (for testing)
// =============================================================================

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !!!                         FOR TESTING ONLY                             !!!
// !!!                                                                       !!!
// !!! This is a mock SQL executor for unit testing the RPC layer.           !!!
// !!! DO NOT use this in production! Use a real SQL database instead.       !!!
// !!!                                                                       !!!
// !!! This mock provides basic SQL parsing to ensure the RPC layer tests    !!!
// !!! don't have false positives from naive regex parsing, but it is NOT    !!!
// !!! a full SQL parser and should never be used for real data.             !!!
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

/**
 * Token types for SQL tokenization
 * @internal FOR TESTING ONLY
 */
type SqlTokenType = 'keyword' | 'identifier' | 'string' | 'number' | 'operator' | 'punctuation' | 'whitespace' | 'comment' | 'parameter';

/**
 * SQL Token
 * @internal FOR TESTING ONLY
 */
interface SqlToken {
  type: SqlTokenType;
  value: string;
  /** Original position in input string */
  position: number;
}

/**
 * Simple SQL tokenizer that respects string literal boundaries
 *
 * FOR TESTING ONLY - This is not a production-grade SQL parser.
 * It handles:
 * - Single and double quoted strings
 * - Escaped quotes ('' and \')
 * - Line comments (--)
 * - Block comments (/* ... *\/)
 * - Basic SQL keywords and identifiers
 *
 * @internal FOR TESTING ONLY
 */
function tokenizeSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  let i = 0;
  const len = sql.length;

  while (i < len) {
    const char = sql[i];
    const remaining = sql.slice(i);

    // Skip null bytes (potential injection attempt)
    if (char === '\x00') {
      i++;
      continue;
    }

    // Whitespace
    if (/\s/.test(char)) {
      let value = char;
      let j = i + 1;
      while (j < len && /\s/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'whitespace', value, position: i });
      i = j;
      continue;
    }

    // Line comment (--)
    if (remaining.startsWith('--')) {
      let value = '--';
      let j = i + 2;
      while (j < len && sql[j] !== '\n') {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'comment', value, position: i });
      i = j;
      continue;
    }

    // Block comment (/* ... */)
    if (remaining.startsWith('/*')) {
      let value = '/*';
      let j = i + 2;
      while (j < len - 1) {
        if (sql[j] === '*' && sql[j + 1] === '/') {
          value += '*/';
          j += 2;
          break;
        }
        value += sql[j];
        j++;
      }
      // Handle unclosed comment - consume rest
      if (j >= len - 1 && !value.endsWith('*/')) {
        value += sql.slice(j);
        j = len;
      }
      tokens.push({ type: 'comment', value, position: i });
      i = j;
      continue;
    }

    // Single-quoted string
    if (char === "'") {
      let value = "'";
      let j = i + 1;
      while (j < len) {
        const c = sql[j];
        // Handle escape sequences
        if (c === "'" && j + 1 < len && sql[j + 1] === "'") {
          // SQL standard: '' escapes to single '
          value += "''";
          j += 2;
          continue;
        }
        if (c === '\\' && j + 1 < len) {
          // Backslash escape (non-standard but common)
          value += c + sql[j + 1];
          j += 2;
          continue;
        }
        if (c === "'") {
          value += "'";
          j++;
          break;
        }
        value += c;
        j++;
      }
      tokens.push({ type: 'string', value, position: i });
      i = j;
      continue;
    }

    // Double-quoted string/identifier
    if (char === '"') {
      let value = '"';
      let j = i + 1;
      while (j < len) {
        const c = sql[j];
        // Handle escape sequences
        if (c === '"' && j + 1 < len && sql[j + 1] === '"') {
          // SQL standard: "" escapes to single "
          value += '""';
          j += 2;
          continue;
        }
        if (c === '\\' && j + 1 < len) {
          // Backslash escape
          value += c + sql[j + 1];
          j += 2;
          continue;
        }
        if (c === '"') {
          value += '"';
          j++;
          break;
        }
        value += c;
        j++;
      }
      tokens.push({ type: 'string', value, position: i });
      i = j;
      continue;
    }

    // Parameter markers ($1, $2, etc.)
    if (char === '$' && /\d/.test(sql[i + 1] || '')) {
      let value = '$';
      let j = i + 1;
      while (j < len && /\d/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'parameter', value, position: i });
      i = j;
      continue;
    }

    // Named parameter (:name)
    if (char === ':' && /[a-zA-Z_]/.test(sql[i + 1] || '')) {
      let value = ':';
      let j = i + 1;
      while (j < len && /[a-zA-Z0-9_]/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'parameter', value, position: i });
      i = j;
      continue;
    }

    // Positional parameter (?)
    if (char === '?') {
      tokens.push({ type: 'parameter', value: '?', position: i });
      i++;
      continue;
    }

    // Numbers
    if (/\d/.test(char)) {
      let value = char;
      let j = i + 1;
      while (j < len && /[\d.]/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'number', value, position: i });
      i = j;
      continue;
    }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(char)) {
      let value = char;
      let j = i + 1;
      while (j < len && /[a-zA-Z0-9_]/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      const upper = value.toUpperCase();
      const keywords = [
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
        'DELETE', 'CREATE', 'TABLE', 'DROP', 'ALTER', 'INDEX', 'JOIN', 'LEFT',
        'RIGHT', 'INNER', 'OUTER', 'ON', 'AND', 'OR', 'NOT', 'IN', 'LIKE',
        'BETWEEN', 'IS', 'NULL', 'TRUE', 'FALSE', 'AS', 'ORDER', 'BY', 'ASC',
        'DESC', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'GROUP', 'HAVING',
        'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION',
      ];
      const type: SqlTokenType = keywords.includes(upper) ? 'keyword' : 'identifier';
      tokens.push({ type, value, position: i });
      i = j;
      continue;
    }

    // Semicolon (statement separator)
    if (char === ';') {
      tokens.push({ type: 'punctuation', value: ';', position: i });
      i++;
      continue;
    }

    // Other operators and punctuation
    const operators = ['(', ')', ',', '.', '=', '<', '>', '!', '+', '-', '*', '/', '%'];
    if (operators.includes(char)) {
      // Handle multi-character operators
      let value = char;
      if ((char === '<' || char === '>' || char === '!' || char === '=') && sql[i + 1] === '=') {
        value += '=';
        i++;
      } else if (char === '<' && sql[i + 1] === '>') {
        value += '>';
        i++;
      }
      tokens.push({ type: 'operator', value, position: i });
      i++;
      continue;
    }

    // Unknown character - skip it
    i++;
  }

  return tokens;
}

/**
 * Check if SQL contains multiple statements (semicolon outside of strings/comments)
 * @internal FOR TESTING ONLY
 */
function hasMultipleStatements(tokens: SqlToken[]): boolean {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');
  const semicolonIndex = significantTokens.findIndex(t => t.type === 'punctuation' && t.value === ';');

  if (semicolonIndex === -1) return false;

  // Check if there's anything meaningful after the semicolon
  const afterSemicolon = significantTokens.slice(semicolonIndex + 1);
  return afterSemicolon.some(t => t.type !== 'whitespace' && t.type !== 'comment');
}

/**
 * Extract table name from SELECT statement tokens
 * @internal FOR TESTING ONLY
 */
function extractTableFromSelect(tokens: SqlToken[]): string | null {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');

  for (let i = 0; i < significantTokens.length; i++) {
    const token = significantTokens[i];
    if (token.type === 'keyword' && token.value.toUpperCase() === 'FROM') {
      const nextToken = significantTokens[i + 1];
      if (nextToken && nextToken.type === 'identifier') {
        return nextToken.value.toLowerCase();
      }
      // Check for subquery
      if (nextToken && nextToken.type === 'operator' && nextToken.value === '(') {
        return null; // Subquery not supported
      }
    }
  }
  return null;
}

/**
 * Extract table name from CREATE TABLE statement tokens
 * @internal FOR TESTING ONLY
 */
function extractTableFromCreateTable(tokens: SqlToken[]): string | null {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');

  for (let i = 0; i < significantTokens.length - 1; i++) {
    const token = significantTokens[i];
    if (token.type === 'keyword' && token.value.toUpperCase() === 'TABLE') {
      const nextToken = significantTokens[i + 1];
      if (nextToken && nextToken.type === 'identifier') {
        return nextToken.value.toLowerCase();
      }
    }
  }
  return null;
}

/**
 * Get the first keyword from tokens to determine statement type
 * @internal FOR TESTING ONLY
 */
function getStatementType(tokens: SqlToken[]): string | null {
  const firstKeyword = tokens.find(t => t.type === 'keyword');
  return firstKeyword?.value.toUpperCase() || null;
}

/**
 * Check if tokens contain a UNION keyword (outside of strings)
 * @internal FOR TESTING ONLY
 */
function hasUnionKeyword(tokens: SqlToken[]): boolean {
  return tokens.some(t => t.type === 'keyword' && t.value.toUpperCase() === 'UNION');
}

/**
 * Check if query has comma in FROM clause (implicit join)
 * @internal FOR TESTING ONLY
 */
function hasImplicitJoin(tokens: SqlToken[]): boolean {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');

  let inFromClause = false;
  for (const token of significantTokens) {
    if (token.type === 'keyword') {
      const upper = token.value.toUpperCase();
      if (upper === 'FROM') {
        inFromClause = true;
        continue;
      }
      // End of FROM clause
      if (inFromClause && ['WHERE', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION'].includes(upper)) {
        inFromClause = false;
      }
    }
    if (inFromClause && token.type === 'operator' && token.value === ',') {
      return true;
    }
  }
  return false;
}

/**
 * Mock query executor for testing
 *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * !!!                         FOR TESTING ONLY                             !!!
 * !!!                                                                       !!!
 * !!! This mock executor uses proper tokenization to parse SQL, but it is   !!!
 * !!! NOT a complete SQL implementation. It exists solely to test the RPC   !!!
 * !!! layer without depending on a real database.                           !!!
 * !!!                                                                       !!!
 * !!! DO NOT use this in production!                                        !!!
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 * Provides a simple in-memory implementation for testing the RPC layer
 * without a real database.
 */
export class MockQueryExecutor implements QueryExecutor {
  #lsn = 0n;
  #tables: Map<string, { columns: string[]; columnTypes: ColumnType[]; rows: unknown[][] }> = new Map();
  #transactions: Map<string, { active: boolean }> = new Map();

  async execute(
    sql: string,
    _params?: unknown[],
    options?: ExecuteOptions
  ): Promise<ExecuteResult> {
    this.#lsn++;

    // =========================================================================
    // FOR TESTING ONLY: Proper SQL tokenization to avoid injection issues
    // =========================================================================

    // Tokenize the SQL to properly handle strings, comments, etc.
    const tokens = tokenizeSql(sql);

    // Reject multi-statement queries (injection prevention)
    if (hasMultipleStatements(tokens)) {
      throw new Error('Multi-statement queries are not supported');
    }

    // Reject UNION queries (common injection vector)
    if (hasUnionKeyword(tokens)) {
      throw new Error('UNION queries are not supported in mock executor');
    }

    // Check for implicit JOINs (comma in FROM clause)
    if (hasImplicitJoin(tokens)) {
      throw new Error('Implicit joins (comma syntax) are not supported');
    }

    const statementType = getStatementType(tokens);

    if (statementType === 'SELECT') {
      // Check for subqueries in FROM
      const fromIndex = tokens.findIndex(t => t.type === 'keyword' && t.value.toUpperCase() === 'FROM');
      if (fromIndex >= 0) {
        const afterFrom = tokens.slice(fromIndex + 1).filter(t => t.type !== 'whitespace');
        if (afterFrom[0]?.type === 'operator' && afterFrom[0]?.value === '(') {
          throw new Error('subqueries not supported');
        }
      }

      const tableName = extractTableFromSelect(tokens);

      if (tableName && this.#tables.has(tableName)) {
        const table = this.#tables.get(tableName)!;
        let rows = table.rows;

        // Apply limit/offset
        if (options?.offset !== undefined) {
          rows = rows.slice(options.offset);
        }
        if (options?.limit !== undefined) {
          rows = rows.slice(0, options.limit);
        }

        return {
          columns: table.columns,
          columnTypes: table.columnTypes,
          rows,
          rowCount: rows.length,
          lsn: this.#lsn,
        };
      }

      // Return empty result for unknown tables
      return {
        columns: [],
        columnTypes: [],
        rows: [],
        rowCount: 0,
        lsn: this.#lsn,
      };
    }

    if (statementType === 'INSERT') {
      // Mock insert
      return {
        columns: [],
        columnTypes: [],
        rows: [],
        rowCount: 1,
        lsn: this.#lsn,
      };
    }

    if (statementType === 'CREATE') {
      // Mock table creation
      const tableName = extractTableFromCreateTable(tokens);
      if (tableName) {
        this.#tables.set(tableName, {
          columns: ['id'],
          columnTypes: ['number'],
          rows: [],
        });
      }
      return {
        columns: [],
        columnTypes: [],
        rows: [],
        rowCount: 0,
        lsn: this.#lsn,
      };
    }

    // For unsupported statements, throw a consistent error
    throw new Error(`Unsupported SQL: ${sql}`);
  }

  getCurrentLSN(): bigint {
    return this.#lsn;
  }

  async getSchema(tables?: string[]): Promise<TableSchema[]> {
    const result: TableSchema[] = [];

    for (const [name, data] of this.#tables) {
      if (tables && !tables.includes(name)) continue;

      result.push({
        name,
        columns: data.columns.map((col, i) => ({
          name: col,
          type: data.columnTypes[i],
          nullable: true,
        })),
        primaryKey: ['id'],
      });
    }

    return result;
  }

  async beginTransaction(_options?: TransactionOptions): Promise<string> {
    const txId = `tx_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    this.#transactions.set(txId, { active: true });
    return txId;
  }

  async commit(txId: string): Promise<void> {
    const tx = this.#transactions.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);
    if (!tx.active) throw new Error(`Transaction ${txId} already completed`);
    tx.active = false;
  }

  async rollback(txId: string, _savepoint?: string): Promise<void> {
    const tx = this.#transactions.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);
    if (!tx.active) throw new Error(`Transaction ${txId} already completed`);
    tx.active = false;
  }

  // Test helper methods
  addTable(name: string, columns: string[], columnTypes: ColumnType[], rows: unknown[][] = []) {
    this.#tables.set(name.toLowerCase(), { columns, columnTypes, rows });
  }
}

// =============================================================================
// Re-exports
// =============================================================================

export type {
  DoSQLAPI,
  QueryRequest,
  QueryResponse,
  StreamRequest,
  StreamChunk,
  CDCRequest,
  CDCEvent,
  TransactionHandle,
  TransactionResult,
  BatchRequest,
  BatchResponse,
  SchemaRequest,
  SchemaResponse,
  RPCError,
};
