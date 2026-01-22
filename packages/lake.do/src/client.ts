/**
 * @dotdo/lake.do - CapnWeb RPC Client for DoLake
 *
 * @packageDocumentation
 */

import type {
  LakeClient,
  LakeQueryOptions,
  LakeQueryResult,
  CDCStreamOptions,
  CDCBatch,
  TableMetadata,
  PartitionInfo,
  PartitionKey,
  CompactionConfig,
  CompactionJob,
  CompactionJobId,
  Snapshot,
  LakeRPCRequest,
  LakeRPCResponse,
  LakeRPCError,
  RetryConfig,
} from './types.js';
import { createPartitionKey, createCompactionJobId, createSnapshotId, DEFAULT_RETRY_CONFIG } from './types.js';

// =============================================================================
// Event Types
// =============================================================================

/**
 * Event types emitted by the DoLakeClient.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type LakeClientEventType = 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'error';

/**
 * Event handler function type for client events.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type LakeClientEventHandler = (event?: unknown) => void;

// =============================================================================
// Client Configuration
// =============================================================================

/**
 * Configuration options for creating a DoLake client.
 *
 * @description Specifies the connection endpoint, authentication credentials,
 * and optional timeout and retry settings for the client.
 *
 * @example
 * ```typescript
 * const config: LakeClientConfig = {
 *   url: 'https://lake.example.com',
 *   token: 'your-auth-token',
 *   timeout: 30000,
 *   retry: {
 *     maxRetries: 3,
 *     baseDelayMs: 100,
 *     maxDelayMs: 5000,
 *   },
 * };
 *
 * const client = createLakeClient(config);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface LakeClientConfig {
  /** DoLake endpoint URL (HTTP or HTTPS, automatically converted to WebSocket) */
  url: string;
  /** Authentication token for API access */
  token?: string;
  /** Request timeout in milliseconds (default: 30000) */
  timeout?: number;
  /** Retry configuration for handling transient failures */
  retry?: RetryConfig;
}

// RetryConfig is imported from @dotdo/shared-types via ./types.js

// =============================================================================
// CapnWeb RPC Client
// =============================================================================

/**
 * Lakehouse client implementation for DoLake using CapnWeb RPC over WebSocket.
 *
 * @description The main client class for interacting with a DoLake lakehouse.
 * Provides methods for querying data, subscribing to CDC streams, managing
 * partitions, triggering compaction, and time travel operations.
 *
 * Use {@link createLakeClient} factory function to create an instance (recommended).
 *
 * @example
 * ```typescript
 * import { createLakeClient } from '@dotdo/lake.do';
 *
 * const client = createLakeClient({
 *   url: 'https://lake.example.com',
 *   token: 'your-token',
 * });
 *
 * // Execute queries
 * const result = await client.query('SELECT * FROM orders WHERE status = $1', {
 *   limit: 100,
 * });
 *
 * // Subscribe to CDC stream
 * for await (const batch of client.subscribe({ tables: ['orders'] })) {
 *   console.log(`Received ${batch.events.length} events`);
 * }
 *
 * // Clean up when done
 * await client.close();
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class DoLakeClient implements LakeClient {
  private readonly config: Required<Omit<LakeClientConfig, 'token'>> & { token?: string };
  private requestId = 0;
  private ws: WebSocket | null = null;
  private cdcStream: CDCStreamController | null = null;
  private pendingRequests = new Map<string, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    timeout: ReturnType<typeof setTimeout>;
  }>();
  private readonly eventListeners = new Map<LakeClientEventType, Set<LakeClientEventHandler>>();
  private _isConnected = false;

  /**
   * Creates a new DoLakeClient instance.
   *
   * @description Initializes the client with the provided configuration.
   * The WebSocket connection is established lazily on the first operation.
   *
   * @param config - Client configuration options
   * @throws {Error} When the URL is empty or invalid
   *
   * @example
   * ```typescript
   * const client = new DoLakeClient({
   *   url: 'https://lake.example.com',
   *   token: 'your-token',
   *   timeout: 30000,
   * });
   * ```
   */
  constructor(config: LakeClientConfig) {
    // Validate URL
    if (!config.url || config.url.trim() === '') {
      throw new Error('URL is required and cannot be empty');
    }

    // Validate URL format
    try {
      const url = new URL(config.url);
      if (!url.protocol.match(/^https?:$/)) {
        throw new Error('URL must use http or https protocol');
      }
    } catch (e) {
      if (e instanceof TypeError) {
        throw new Error(`Invalid URL format: ${config.url}`);
      }
      throw e;
    }

    this.config = {
      ...config,
      timeout: config.timeout ?? 30000,
      retry: config.retry ?? DEFAULT_RETRY_CONFIG,
    };
  }

  // ===========================================================================
  // Public Properties
  // ===========================================================================

  /**
   * Indicates whether the client is currently connected to the server.
   *
   * @description Returns true if the WebSocket connection is open and ready
   * to send/receive messages. Returns false otherwise.
   *
   * @example
   * ```typescript
   * const client = createLakeClient({ url: 'https://lake.example.com' });
   * console.log(client.isConnected); // false
   * await client.connect();
   * console.log(client.isConnected); // true
   * ```
   *
   * @public
   * @readonly
   */
  get isConnected(): boolean {
    return this._isConnected && this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  /**
   * Returns the configured URL for this client.
   *
   * @example
   * ```typescript
   * const client = createLakeClient({ url: 'https://lake.example.com' });
   * console.log(client.url); // 'https://lake.example.com'
   * ```
   *
   * @public
   * @readonly
   */
  get url(): string {
    return this.config.url;
  }

  // ===========================================================================
  // Event Emitter Methods
  // ===========================================================================

  /**
   * Registers an event listener for the specified event type.
   *
   * @description Adds a callback function that will be called when the
   * specified event is emitted. Multiple listeners can be registered for
   * the same event.
   *
   * @param event - The event type to listen for
   * @param handler - The callback function to invoke when the event occurs
   *
   * @example
   * ```typescript
   * client.on('connected', () => console.log('Connected!'));
   * client.on('disconnected', () => console.log('Disconnected!'));
   * client.on('error', (error) => console.error('Error:', error));
   * ```
   *
   * @public
   */
  on(event: LakeClientEventType, handler: LakeClientEventHandler): void {
    if (!this.eventListeners.has(event)) {
      this.eventListeners.set(event, new Set());
    }
    this.eventListeners.get(event)!.add(handler);
  }

  /**
   * Removes an event listener for the specified event type.
   *
   * @description Removes a previously registered callback function.
   * If the handler was not registered, this method does nothing.
   *
   * @param event - The event type to remove the listener from
   * @param handler - The callback function to remove
   *
   * @example
   * ```typescript
   * const handler = () => console.log('Connected!');
   * client.on('connected', handler);
   * // Later...
   * client.off('connected', handler);
   * ```
   *
   * @public
   */
  off(event: LakeClientEventType, handler: LakeClientEventHandler): void {
    const handlers = this.eventListeners.get(event);
    if (handlers) {
      handlers.delete(handler);
    }
  }

  /**
   * Registers a one-time event listener for the specified event type.
   *
   * @description Adds a callback function that will be called only once
   * when the specified event is emitted, then automatically removed.
   *
   * @param event - The event type to listen for
   * @param handler - The callback function to invoke when the event occurs
   *
   * @example
   * ```typescript
   * client.once('connected', () => {
   *   console.log('First connection established!');
   * });
   * ```
   *
   * @public
   */
  once(event: LakeClientEventType, handler: LakeClientEventHandler): void {
    const onceHandler: LakeClientEventHandler = (data) => {
      this.off(event, onceHandler);
      handler(data);
    };
    this.on(event, onceHandler);
  }

  /**
   * Emits an event to all registered listeners.
   *
   * @param event - The event type to emit
   * @param data - Optional data to pass to the listeners
   *
   * @internal
   */
  private emit(event: LakeClientEventType, data?: unknown): void {
    const handlers = this.eventListeners.get(event);
    if (handlers) {
      for (const handler of handlers) {
        try {
          handler(data);
        } catch (error) {
          console.error(`Error in ${event} event handler:`, error);
        }
      }
    }
  }

  // ===========================================================================
  // Connection Management (Public)
  // ===========================================================================

  /**
   * Explicitly establishes a WebSocket connection to the server.
   *
   * @description Connects to the DoLake server. This method is optional as
   * the client will automatically connect on the first operation. However,
   * calling connect() explicitly allows you to handle connection errors
   * before attempting operations.
   *
   * @returns Resolves when the connection is established
   * @throws {ConnectionError} When the connection fails
   *
   * @example
   * ```typescript
   * const client = createLakeClient({ url: 'https://lake.example.com' });
   * try {
   *   await client.connect();
   *   console.log('Connected successfully!');
   * } catch (error) {
   *   console.error('Failed to connect:', error);
   * }
   * ```
   *
   * @public
   */
  async connect(): Promise<void> {
    await this.ensureConnection();
  }

  /**
   * Closes the client connection.
   *
   * @description Alias for {@link close}. Closes the WebSocket connection,
   * cancels any pending requests, and stops any active CDC subscriptions.
   *
   * @returns Resolves when the connection is fully closed
   *
   * @example
   * ```typescript
   * await client.disconnect();
   * console.log(client.isConnected); // false
   * ```
   *
   * @public
   */
  async disconnect(): Promise<void> {
    return this.close();
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  private async ensureConnection(): Promise<WebSocket> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return this.ws;
    }

    return new Promise((resolve, reject) => {
      const wsUrl = this.config.url.replace(/^http/, 'ws');
      this.ws = new WebSocket(wsUrl);

      this.ws.addEventListener('open', () => {
        this._isConnected = true;
        this.emit('connected');
        resolve(this.ws!);
      });

      this.ws.addEventListener('error', (event: Event) => {
        this._isConnected = false;
        const error = new ConnectionError({
          code: 'CONNECTION_ERROR',
          message: `WebSocket error: ${event}`,
        });
        this.emit('error', error);
        reject(error);
      });

      this.ws.addEventListener('close', () => {
        const wasConnected = this._isConnected;
        this._isConnected = false;
        this.ws = null;

        // Emit disconnected event if we were previously connected
        if (wasConnected) {
          this.emit('disconnected');
        }

        // Reject all pending requests
        for (const [id, pending] of this.pendingRequests) {
          clearTimeout(pending.timeout);
          pending.reject(new ConnectionError({
            code: 'CONNECTION_CLOSED',
            message: 'Connection closed',
          }));
          this.pendingRequests.delete(id);
        }
        // Close CDC stream
        if (this.cdcStream) {
          this.cdcStream.close();
          this.cdcStream = null;
        }
      });

      this.ws.addEventListener('message', (event: MessageEvent) => {
        this.handleMessage(event.data as string | ArrayBuffer);
      });
    });
  }

  private handleMessage(data: string | ArrayBuffer): void {
    try {
      const message = typeof data === 'string' ? data : new TextDecoder().decode(data);
      const parsed = JSON.parse(message);

      // Handle CDC stream messages
      if (parsed.type === 'cdc_batch' && this.cdcStream) {
        this.cdcStream.push(parsed as CDCBatch);
        return;
      }

      // Handle RPC responses
      const response = parsed as LakeRPCResponse;
      const pending = this.pendingRequests.get(response.id);
      if (pending) {
        clearTimeout(pending.timeout);
        this.pendingRequests.delete(response.id);

        if (response.error) {
          pending.reject(new LakeError(response.error));
        } else {
          pending.resolve(response.result);
        }
      }
    } catch (error) {
      console.error('Failed to parse RPC response:', error);
    }
  }

  private async rpc<T>(method: string, params: unknown): Promise<T> {
    const ws = await this.ensureConnection();
    const id = `${++this.requestId}`;

    const request: LakeRPCRequest = {
      id,
      method: method as LakeRPCRequest['method'],
      params,
    };

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`Request timeout: ${method}`));
      }, this.config.timeout);

      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeout,
      });

      ws.send(JSON.stringify(request));
    });
  }

  // ===========================================================================
  // LakeClient Implementation
  // ===========================================================================

  /**
   * Executes a SQL query against the lakehouse.
   *
   * @description Runs a SQL query and returns the results. Supports time travel,
   * partition pruning, and column projection through query options.
   *
   * @typeParam T - The expected row type in the result set
   *
   * @param sql - The SQL query to execute
   * @param options - Optional query configuration (time travel, partitions, etc.)
   * @returns Query results including rows and execution statistics
   * @throws {LakeError} When the query fails (syntax error, table not found, etc.)
   *
   * @example
   * ```typescript
   * // Simple query
   * const result = await client.query('SELECT * FROM orders LIMIT 10');
   * console.log(`Found ${result.rowCount} orders`);
   *
   * // Typed query with options
   * interface Order { id: string; amount: number; status: string; }
   * const orders = await client.query<Order>(
   *   'SELECT id, amount, status FROM orders WHERE amount > 100',
   *   { limit: 50, columns: ['id', 'amount', 'status'] }
   * );
   *
   * // Time travel query
   * const historical = await client.query(
   *   'SELECT * FROM inventory',
   *   { asOf: new Date('2024-01-01') }
   * );
   * ```
   */
  async query<T = Record<string, unknown>>(
    sql: string,
    options?: LakeQueryOptions
  ): Promise<LakeQueryResult<T>> {
    return this.rpc<LakeQueryResult<T>>('query', { sql, ...options });
  }

  /**
   * Subscribes to a CDC (Change Data Capture) event stream.
   *
   * @description Creates an async iterable that yields batches of CDC events
   * as they occur. The stream continues until explicitly closed, the connection
   * is lost, or an error occurs. Automatically unsubscribes when iteration stops.
   *
   * @param options - Optional stream configuration (tables, operations, etc.)
   * @returns An async iterable that yields CDC event batches
   * @throws {LakeError} When subscription fails or connection is lost
   *
   * @example
   * ```typescript
   * // Subscribe to all changes
   * for await (const batch of client.subscribe()) {
   *   console.log(`Batch #${batch.sequenceNumber}: ${batch.events.length} events`);
   *   for (const event of batch.events) {
   *     console.log(`${event.operation} on ${event.table}:`, event.after);
   *   }
   * }
   *
   * // Subscribe to specific tables with filtering
   * for await (const batch of client.subscribe({
   *   tables: ['orders', 'inventory'],
   *   operations: ['INSERT', 'UPDATE'],
   *   batchSize: 100,
   * })) {
   *   await processBatch(batch);
   * }
   * ```
   */
  async *subscribe(options?: CDCStreamOptions): AsyncIterable<CDCBatch> {
    await this.ensureConnection();

    // Subscribe to CDC stream
    await this.rpc<void>('subscribe', options ?? {});

    // Create stream controller
    this.cdcStream = new CDCStreamController();

    try {
      yield* this.cdcStream;
    } finally {
      // Unsubscribe when done
      await this.rpc<void>('unsubscribe', {}).catch(() => {});
      this.cdcStream = null;
    }
  }

  /**
   * Retrieves metadata for a table.
   *
   * @description Returns complete metadata including schema, partition spec,
   * snapshots, and custom properties. Useful for schema introspection and
   * discovering time travel capabilities.
   *
   * @param tableName - The name of the table
   * @returns Table metadata including schema, partitioning, and snapshots
   * @throws {LakeError} When the table is not found
   *
   * @example
   * ```typescript
   * const metadata = await client.getMetadata('orders');
   *
   * console.log(`Table: ${metadata.tableId}`);
   * console.log(`Columns: ${metadata.schema.columns.map(c => c.name).join(', ')}`);
   * console.log(`Partitioned by: ${metadata.partitionSpec.column}`);
   * console.log(`Current snapshot: ${metadata.currentSnapshotId}`);
   * console.log(`Total snapshots: ${metadata.snapshots.length}`);
   * ```
   */
  async getMetadata(tableName: string): Promise<TableMetadata> {
    const result = await this.rpc<TableMetadata>('getMetadata', { tableName });
    // Convert snapshot IDs
    const metadata: TableMetadata = {
      tableId: result.tableId,
      schema: result.schema,
      partitionSpec: result.partitionSpec,
      properties: result.properties,
      snapshots: result.snapshots.map((s): Snapshot => {
        const snapshot: Snapshot = {
          id: createSnapshotId(s.id as unknown as string),
          timestamp: new Date(s.timestamp),
          summary: s.summary,
          manifestList: s.manifestList,
        };
        if (s.parentId) {
          snapshot.parentId = createSnapshotId(s.parentId as unknown as string);
        }
        return snapshot;
      }),
    };
    if (result.currentSnapshotId) {
      metadata.currentSnapshotId = createSnapshotId(result.currentSnapshotId as unknown as string);
    }
    return metadata;
  }

  /**
   * Lists all partitions for a table.
   *
   * @description Returns information about each partition including file count,
   * row count, and size. Useful for monitoring data distribution and deciding
   * when to trigger compaction.
   *
   * @param tableName - The name of the table
   * @returns Array of partition information objects
   * @throws {LakeError} When the table is not found
   *
   * @example
   * ```typescript
   * const partitions = await client.listPartitions('orders');
   *
   * for (const partition of partitions) {
   *   console.log(`Partition ${partition.key}:`);
   *   console.log(`  Files: ${partition.fileCount}`);
   *   console.log(`  Rows: ${partition.rowCount}`);
   *   console.log(`  Size: ${(partition.sizeBytes / 1024 / 1024).toFixed(2)} MB`);
   *
   *   // Check if compaction is needed
   *   if (partition.fileCount > 10) {
   *     console.log('  -> Needs compaction');
   *   }
   * }
   * ```
   */
  async listPartitions(tableName: string): Promise<PartitionInfo[]> {
    const result = await this.rpc<PartitionInfo[]>('listPartitions', { tableName });
    return result.map((p) => ({
      ...p,
      key: createPartitionKey(p.key as unknown as string),
      lastModified: new Date(p.lastModified),
    }));
  }

  /**
   * Triggers a compaction job for a partition.
   *
   * @description Initiates a background job to merge small files into larger,
   * more efficient files. Returns immediately with job metadata; use
   * {@link getCompactionStatus} to poll for completion.
   *
   * @param partition - The partition key to compact
   * @param config - Optional compaction configuration
   * @returns The created compaction job with initial status
   * @throws {LakeError} When the partition is not found or compaction fails to start
   *
   * @example
   * ```typescript
   * const partitions = await client.listPartitions('orders');
   * const partition = partitions.find(p => p.fileCount > 10);
   *
   * if (partition) {
   *   const job = await client.compact(partition.key, {
   *     targetFileSize: 128 * 1024 * 1024, // 128 MB
   *     maxFiles: 50,
   *   });
   *
   *   console.log(`Started compaction job: ${job.id}`);
   *   console.log(`Compacting ${job.inputFiles.length} files`);
   * }
   * ```
   */
  async compact(
    partition: PartitionKey,
    config?: Partial<CompactionConfig>
  ): Promise<CompactionJob> {
    const result = await this.rpc<CompactionJob>('compact', { partition, config });
    return this.transformCompactionJob(result);
  }

  /**
   * Gets the current status of a compaction job.
   *
   * @description Retrieves the latest status of a running or completed compaction job.
   * Use this to poll for job completion after calling {@link compact}.
   *
   * @param jobId - The compaction job ID
   * @returns Current job status and metadata
   * @throws {LakeError} When the job is not found
   *
   * @example
   * ```typescript
   * const job = await client.compact(partitionKey);
   *
   * // Poll until complete
   * let status = job;
   * while (status.status === 'pending' || status.status === 'running') {
   *   await new Promise(resolve => setTimeout(resolve, 1000));
   *   status = await client.getCompactionStatus(job.id);
   *   console.log(`Job ${job.id}: ${status.status}`);
   * }
   *
   * if (status.status === 'completed') {
   *   console.log(`Compacted ${status.inputFiles.length} -> ${status.outputFiles?.length} files`);
   *   console.log(`Read ${status.bytesRead} bytes, wrote ${status.bytesWritten} bytes`);
   * } else {
   *   console.error(`Compaction failed: ${status.error}`);
   * }
   * ```
   */
  async getCompactionStatus(jobId: CompactionJobId): Promise<CompactionJob> {
    const result = await this.rpc<CompactionJob>('getCompactionStatus', { jobId });
    return this.transformCompactionJob(result);
  }

  private transformCompactionJob(job: CompactionJob): CompactionJob {
    const result: CompactionJob = {
      id: createCompactionJobId(job.id as unknown as string),
      partition: createPartitionKey(job.partition as unknown as string),
      status: job.status,
      inputFiles: job.inputFiles,
    };
    if (job.outputFiles) {
      result.outputFiles = job.outputFiles;
    }
    if (job.error) {
      result.error = job.error;
    }
    if (job.bytesRead !== undefined) {
      result.bytesRead = job.bytesRead;
    }
    if (job.bytesWritten !== undefined) {
      result.bytesWritten = job.bytesWritten;
    }
    if (job.startedAt) {
      result.startedAt = new Date(job.startedAt);
    }
    if (job.completedAt) {
      result.completedAt = new Date(job.completedAt);
    }
    return result;
  }

  /**
   * Lists all snapshots for a table.
   *
   * @description Returns the history of snapshots for time travel queries.
   * Snapshots are ordered from newest to oldest. Each snapshot represents
   * a consistent point-in-time view of the table.
   *
   * @param tableName - The name of the table
   * @returns Array of snapshots ordered by timestamp descending
   * @throws {LakeError} When the table is not found
   *
   * @example
   * ```typescript
   * const snapshots = await client.listSnapshots('orders');
   *
   * console.log(`Table has ${snapshots.length} snapshots`);
   *
   * for (const snapshot of snapshots) {
   *   console.log(`Snapshot ${snapshot.id}:`);
   *   console.log(`  Created: ${snapshot.timestamp}`);
   *   console.log(`  Changes: +${snapshot.summary.addedRows}/-${snapshot.summary.deletedRows} rows`);
   * }
   *
   * // Query at the oldest snapshot
   * if (snapshots.length > 0) {
   *   const oldest = snapshots[snapshots.length - 1];
   *   const result = await client.query('SELECT COUNT(*) as count FROM orders', {
   *     asOf: oldest.id,
   *   });
   *   console.log(`Orders at ${oldest.timestamp}: ${result.rows[0].count}`);
   * }
   * ```
   */
  async listSnapshots(tableName: string): Promise<Snapshot[]> {
    const result = await this.rpc<Snapshot[]>('listSnapshots', { tableName });
    return result.map((s): Snapshot => {
      const snapshot: Snapshot = {
        id: createSnapshotId(s.id as unknown as string),
        timestamp: new Date(s.timestamp),
        summary: s.summary,
        manifestList: s.manifestList,
      };
      if (s.parentId) {
        snapshot.parentId = createSnapshotId(s.parentId as unknown as string);
      }
      return snapshot;
    });
  }

  /**
   * Checks the connection health and measures round-trip latency.
   *
   * @description Sends a ping to the server and measures the time for a response.
   * Useful for health checks, connection monitoring, and latency measurements.
   *
   * @returns Object containing latency in milliseconds
   * @throws {LakeError} When the connection is not available or times out
   *
   * @example
   * ```typescript
   * try {
   *   const { latency } = await client.ping();
   *   console.log(`Connection healthy, latency: ${latency.toFixed(2)}ms`);
   * } catch (error) {
   *   console.error('Connection unhealthy:', error);
   * }
   *
   * // Periodic health check
   * setInterval(async () => {
   *   const { latency } = await client.ping();
   *   if (latency > 100) {
   *     console.warn(`High latency detected: ${latency}ms`);
   *   }
   * }, 30000);
   * ```
   */
  async ping(): Promise<{ latency: number }> {
    const start = performance.now();
    await this.rpc<void>('ping', {});
    return { latency: performance.now() - start };
  }

  /**
   * Closes the client connection and cleans up resources.
   *
   * @description Closes the WebSocket connection, cancels any pending requests,
   * and stops any active CDC subscriptions. Always call this when done with the
   * client to prevent resource leaks.
   *
   * @returns Resolves when the connection is fully closed
   *
   * @example
   * ```typescript
   * const client = createLakeClient({ url: 'https://lake.example.com' });
   *
   * try {
   *   const result = await client.query('SELECT * FROM orders');
   *   // Process results...
   * } finally {
   *   await client.close();
   * }
   *
   * // Or with async/await error handling
   * const client = createLakeClient({ url: 'https://lake.example.com' });
   * await doWork(client);
   * await client.close();
   * ```
   */
  async close(): Promise<void> {
    const wasConnected = this._isConnected;

    if (this.cdcStream) {
      this.cdcStream.close();
      this.cdcStream = null;
    }
    if (this.ws) {
      this._isConnected = false;
      this.ws.close();
      this.ws = null;
    }

    // Emit disconnected event if we were previously connected
    if (wasConnected) {
      this.emit('disconnected');
    }
  }
}

// =============================================================================
// CDC Stream Controller
// =============================================================================

/**
 * Backpressure strategy for handling queue overflow.
 *
 * @description Controls how the controller handles new events when the queue is full.
 * - `block`: push() returns false, pushAsync() waits for space
 * - `drop-oldest`: Removes oldest batch to make room for new one
 * - `drop-newest`: Rejects new batch without modifying queue
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type BackpressureStrategy = 'block' | 'drop-oldest' | 'drop-newest';

/**
 * Metrics about queue depth and throughput.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface CDCStreamMetrics {
  /** Current number of batches in the queue */
  currentDepth: number;
  /** Maximum configured queue size */
  maxDepth: number;
  /** Highest queue depth seen since creation or last reset */
  peakDepth: number;
  /** Number of batches dropped due to backpressure */
  droppedCount: number;
  /** Total number of batches pushed */
  totalPushed: number;
  /** Total number of batches consumed */
  totalConsumed: number;
  /** Queue utilization as a percentage (0-100) */
  utilizationPercent: number;
}

/**
 * Water mark event payload.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface WaterMarkEvent {
  currentDepth: number;
  maxDepth: number;
  utilizationPercent: number;
}

/**
 * Configuration options for CDCStreamController.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface CDCStreamControllerOptions {
  /** Maximum number of batches to buffer (default: 1000, minimum: 1) */
  maxQueueSize?: number;
  /** Strategy when queue is full (default: 'block') */
  backpressureStrategy?: BackpressureStrategy;
  /** High water mark threshold percentage (0-100) for emitting event */
  highWaterMark?: number;
  /** Low water mark threshold percentage (0-100) for emitting event */
  lowWaterMark?: number;
  /** Callback when queue reaches high water mark */
  onHighWaterMark?: (event: WaterMarkEvent) => void;
  /** Callback when queue falls below low water mark */
  onLowWaterMark?: (event: WaterMarkEvent) => void;
}

/**
 * Controller for CDC stream iteration with bounded queue and backpressure support.
 *
 * @description Manages CDC event batches with configurable queue bounds,
 * backpressure strategies, and metrics for monitoring. The queue prevents
 * unbounded memory growth under high throughput scenarios.
 *
 * @example
 * ```typescript
 * const controller = new CDCStreamController({
 *   maxQueueSize: 100,
 *   backpressureStrategy: 'drop-oldest',
 *   highWaterMark: 80,
 *   onHighWaterMark: (event) => console.warn('Queue filling up:', event),
 * });
 *
 * // Check if push succeeded
 * if (!controller.push(batch)) {
 *   console.warn('Queue full, batch rejected');
 * }
 *
 * // Or wait for space
 * await controller.pushAsync(batch);
 *
 * // Monitor metrics
 * const metrics = controller.getMetrics();
 * console.log(`Queue utilization: ${metrics.utilizationPercent}%`);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class CDCStreamController implements AsyncIterable<CDCBatch> {
  private readonly queue: CDCBatch[] = [];
  private resolvers: Array<(value: IteratorResult<CDCBatch>) => void> = [];
  private asyncPushResolvers: Array<() => void> = [];
  private closed = false;

  private _maxQueueSize: number;
  private readonly _backpressureStrategy: BackpressureStrategy;
  private readonly _highWaterMark: number | undefined;
  private readonly _lowWaterMark: number | undefined;
  private readonly _onHighWaterMark: ((event: WaterMarkEvent) => void) | undefined;
  private readonly _onLowWaterMark: ((event: WaterMarkEvent) => void) | undefined;

  private _droppedCount = 0;
  private _peakDepth = 0;
  private _totalPushed = 0;
  private _totalConsumed = 0;
  private _wasAboveHighWaterMark = false;

  /**
   * Creates a new CDCStreamController instance.
   *
   * @param options - Configuration options
   * @throws {Error} When maxQueueSize is less than 1
   */
  constructor(options: CDCStreamControllerOptions = {}) {
    const maxQueueSize = options.maxQueueSize ?? 1000;
    if (maxQueueSize < 1) {
      throw new Error('maxQueueSize must be at least 1');
    }
    this._maxQueueSize = maxQueueSize;
    this._backpressureStrategy = options.backpressureStrategy ?? 'block';
    this._highWaterMark = options.highWaterMark;
    this._lowWaterMark = options.lowWaterMark;
    this._onHighWaterMark = options.onHighWaterMark;
    this._onLowWaterMark = options.onLowWaterMark;
  }

  /**
   * Maximum number of batches that can be buffered.
   */
  get maxQueueSize(): number {
    return this._maxQueueSize;
  }

  /**
   * Current number of batches in the queue.
   */
  get queueSize(): number {
    return this.queue.length;
  }

  /**
   * Number of batches dropped due to backpressure.
   */
  get droppedCount(): number {
    return this._droppedCount;
  }

  /**
   * Current backpressure strategy.
   */
  get backpressureStrategy(): BackpressureStrategy {
    return this._backpressureStrategy;
  }

  /**
   * Sets a new maximum queue size.
   *
   * @description If the new size is smaller than the current queue size,
   * excess batches are dropped according to the backpressure strategy.
   *
   * @param size - New maximum queue size (must be at least 1)
   * @throws {Error} When size is less than 1
   */
  setMaxQueueSize(size: number): void {
    if (size < 1) {
      throw new Error('maxQueueSize must be at least 1');
    }
    this._maxQueueSize = size;

    // Drop excess batches if needed
    while (this.queue.length > this._maxQueueSize) {
      if (this._backpressureStrategy === 'drop-oldest') {
        this.queue.shift();
      } else {
        this.queue.pop();
      }
      this._droppedCount++;
    }

    this.checkWaterMarks();
  }

  /**
   * Pushes a batch to the queue.
   *
   * @description Behavior depends on the backpressure strategy:
   * - `block`: Returns false when queue is full
   * - `drop-oldest`: Drops oldest batch, always returns true
   * - `drop-newest`: Returns false when full without modifying queue
   *
   * @param batch - The CDC batch to push
   * @returns true if the batch was accepted, false if rejected
   */
  push(batch: CDCBatch): boolean {
    if (this.closed) return false;

    this._totalPushed++;

    // If there's a waiting consumer, deliver directly
    const resolver = this.resolvers.shift();
    if (resolver) {
      resolver({ value: batch, done: false });
      this.updatePeakDepth();
      this.checkWaterMarks();
      return true;
    }

    // Check if queue is full
    if (this.queue.length >= this._maxQueueSize) {
      switch (this._backpressureStrategy) {
        case 'block':
        case 'drop-newest':
          this._droppedCount++;
          this._totalPushed--; // Don't count dropped batches
          return false;

        case 'drop-oldest':
          this.queue.shift();
          this._droppedCount++;
          break;
      }
    }

    this.queue.push(batch);
    this.updatePeakDepth();
    this.checkWaterMarks();
    return true;
  }

  /**
   * Pushes a batch to the queue, waiting for space if necessary.
   *
   * @description When the queue is full, this method waits until a batch
   * is consumed before adding the new batch. Only applies to 'block' strategy;
   * other strategies behave the same as push().
   *
   * @param batch - The CDC batch to push
   * @returns Promise that resolves to true when the batch is accepted
   */
  async pushAsync(batch: CDCBatch): Promise<boolean> {
    if (this.closed) return false;

    // If queue has space, push immediately
    if (this.queue.length < this._maxQueueSize || this._backpressureStrategy !== 'block') {
      return this.push(batch);
    }

    // Wait for space to become available
    await new Promise<void>((resolve) => {
      this.asyncPushResolvers.push(resolve);
    });

    // Now push the batch
    return this.push(batch);
  }

  /**
   * Returns current queue metrics.
   *
   * @returns Object containing queue depth statistics
   */
  getMetrics(): CDCStreamMetrics {
    return {
      currentDepth: this.queue.length,
      maxDepth: this._maxQueueSize,
      peakDepth: this._peakDepth,
      droppedCount: this._droppedCount,
      totalPushed: this._totalPushed,
      totalConsumed: this._totalConsumed,
      utilizationPercent: Math.round((this.queue.length / this._maxQueueSize) * 100),
    };
  }

  /**
   * Resets metrics counters while preserving the queue contents.
   *
   * @description Resets peakDepth, totalPushed, totalConsumed, and droppedCount.
   * The current queue depth becomes the new peak.
   */
  resetMetrics(): void {
    this._peakDepth = this.queue.length;
    this._totalPushed = 0;
    this._totalConsumed = 0;
    this._droppedCount = 0;
  }

  private updatePeakDepth(): void {
    if (this.queue.length > this._peakDepth) {
      this._peakDepth = this.queue.length;
    }
  }

  private checkWaterMarks(): void {
    const utilizationPercent = Math.round((this.queue.length / this._maxQueueSize) * 100);

    // Check high water mark
    if (this._highWaterMark !== undefined) {
      if (utilizationPercent >= this._highWaterMark && !this._wasAboveHighWaterMark) {
        this._wasAboveHighWaterMark = true;
        if (this._onHighWaterMark) {
          this._onHighWaterMark({
            currentDepth: this.queue.length,
            maxDepth: this._maxQueueSize,
            utilizationPercent,
          });
        }
      }
    }

    // Check low water mark
    if (this._lowWaterMark !== undefined) {
      if (utilizationPercent <= this._lowWaterMark && this._wasAboveHighWaterMark) {
        this._wasAboveHighWaterMark = false;
        if (this._onLowWaterMark) {
          this._onLowWaterMark({
            currentDepth: this.queue.length,
            maxDepth: this._maxQueueSize,
            utilizationPercent,
          });
        }
      }
    }
  }

  private notifyAsyncPushWaiters(): void {
    const waiter = this.asyncPushResolvers.shift();
    if (waiter) {
      waiter();
    }
  }

  close(): void {
    this.closed = true;
    for (const resolver of this.resolvers) {
      resolver({ value: undefined as unknown as CDCBatch, done: true });
    }
    this.resolvers = [];

    // Also resolve any waiting async pushers
    for (const waiter of this.asyncPushResolvers) {
      waiter();
    }
    this.asyncPushResolvers = [];
  }

  [Symbol.asyncIterator](): AsyncIterator<CDCBatch> {
    return {
      next: (): Promise<IteratorResult<CDCBatch>> => {
        if (this.closed && this.queue.length === 0) {
          return Promise.resolve({ value: undefined as unknown as CDCBatch, done: true });
        }

        const batch = this.queue.shift();
        if (batch) {
          this._totalConsumed++;
          this.checkWaterMarks();
          this.notifyAsyncPushWaiters();
          return Promise.resolve({ value: batch, done: false });
        }

        return new Promise((resolve) => {
          this.resolvers.push((result) => {
            if (!result.done) {
              this._totalConsumed++;
              this.checkWaterMarks();
              this.notifyAsyncPushWaiters();
            }
            resolve(result);
          });
        });
      },
    };
  }
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error thrown by Lake operations.
 *
 * @description Represents an error returned by the DoLake server. Contains a
 * machine-readable error code and optional details for debugging. All lake.do
 * API methods may throw this error type.
 *
 * Common error codes:
 * - `TABLE_NOT_FOUND`: The requested table does not exist
 * - `PARTITION_NOT_FOUND`: The requested partition does not exist
 * - `INVALID_SQL`: SQL syntax error or invalid query
 * - `UNAUTHORIZED`: Authentication failed or token expired
 * - `TIMEOUT`: Request exceeded the configured timeout
 * - `CONNECTION_CLOSED`: WebSocket connection was closed
 *
 * @example
 * ```typescript
 * try {
 *   const result = await client.query('SELECT * FROM nonexistent');
 * } catch (error) {
 *   if (error instanceof LakeError) {
 *     console.error(`Lake error [${error.code}]: ${error.message}`);
 *     if (error.code === 'TABLE_NOT_FOUND') {
 *       // Handle missing table
 *     }
 *     if (error.details) {
 *       console.error('Details:', error.details);
 *     }
 *   } else {
 *     throw error;
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class LakeError extends Error {
  /** Machine-readable error code for programmatic error handling */
  readonly code: string;
  /** Additional error context (structure varies by error type) */
  readonly details?: unknown;

  /**
   * Creates a new LakeError instance.
   *
   * @param error - The RPC error response from the server
   */
  constructor(error: LakeRPCError) {
    super(error.message);
    this.name = 'LakeError';
    this.code = error.code;
    this.details = error.details;
  }
}

/**
 * Error thrown when a connection operation fails.
 *
 * @description Represents connection-related errors such as connection failures,
 * timeouts, or disconnections. Extends LakeError for consistent error handling.
 *
 * Common error codes:
 * - `CONNECTION_ERROR`: General connection failure
 * - `CONNECTION_CLOSED`: WebSocket connection was closed unexpectedly
 * - `CONNECTION_TIMEOUT`: Connection attempt timed out
 *
 * @example
 * ```typescript
 * try {
 *   await client.connect();
 * } catch (error) {
 *   if (error instanceof ConnectionError) {
 *     console.error(`Connection failed [${error.code}]: ${error.message}`);
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class ConnectionError extends LakeError {
  /**
   * Creates a new ConnectionError instance.
   *
   * @param error - The error details
   */
  constructor(error: LakeRPCError) {
    super(error);
    this.name = 'ConnectionError';
  }
}

/**
 * Error thrown when a query operation fails.
 *
 * @description Represents query-related errors such as SQL syntax errors,
 * table not found, or query timeouts. Extends LakeError for consistent error handling.
 *
 * Common error codes:
 * - `INVALID_SQL`: SQL syntax error or invalid query
 * - `TABLE_NOT_FOUND`: The requested table does not exist
 * - `QUERY_TIMEOUT`: Query execution timed out
 *
 * @example
 * ```typescript
 * try {
 *   await client.query('INVALID SQL');
 * } catch (error) {
 *   if (error instanceof QueryError) {
 *     console.error(`Query failed [${error.code}]: ${error.message}`);
 *   }
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class QueryError extends LakeError {
  /**
   * Creates a new QueryError instance.
   *
   * @param error - The error details
   */
  constructor(error: LakeRPCError) {
    super(error);
    this.name = 'QueryError';
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Creates a new DoLake client instance.
 *
 * @description Factory function for creating a LakeClient. This is the recommended
 * way to create a client instance. The returned client establishes a WebSocket
 * connection to the DoLake server on the first operation.
 *
 * @param config - Client configuration options
 * @param config.url - DoLake endpoint URL (HTTP/HTTPS, converted to WebSocket)
 * @param config.token - Optional authentication token
 * @param config.timeout - Optional request timeout in milliseconds (default: 30000)
 * @param config.retry - Optional retry configuration for transient failures
 * @returns A new LakeClient instance ready for use
 *
 * @example
 * ```typescript
 * import { createLakeClient } from '@dotdo/lake.do';
 *
 * // Basic usage
 * const client = createLakeClient({
 *   url: 'https://lake.example.com',
 *   token: 'your-token',
 * });
 *
 * // Execute a query
 * const result = await client.query('SELECT * FROM orders LIMIT 10');
 * console.log(`Found ${result.rowCount} orders`);
 *
 * // Always close when done
 * await client.close();
 * ```
 *
 * @example
 * ```typescript
 * // With custom configuration
 * const client = createLakeClient({
 *   url: 'https://lake.example.com',
 *   token: process.env.LAKE_TOKEN,
 *   timeout: 60000, // 60 second timeout
 *   retry: {
 *     maxRetries: 5,
 *     baseDelayMs: 200,
 *     maxDelayMs: 10000,
 *   },
 * });
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createLakeClient(config: LakeClientConfig): LakeClient {
  return new DoLakeClient(config);
}
