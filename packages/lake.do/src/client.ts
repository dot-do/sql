/**
 * @dotdo/lake.do - CapnWeb RPC Client for DoLake
 *
 * This is the main client facade that composes:
 * - WebSocketConnectionManager for connection handling
 * - QueryExecutor for RPC operations
 * - CDCStreamController for CDC streaming
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
  LakeRPCResponse,
  RetryConfig,
} from './types.js';
import { DEFAULT_RETRY_CONFIG } from './types.js';
import { WebSocketConnectionManager, type ConnectionEventHandler } from './connection/index.js';
import { CDCStreamController } from './cdc-stream/index.js';
import { QueryExecutor } from './query/index.js';
import { LakeError, ConnectionError, QueryError, TimeoutError } from './errors.js';
import { DEFAULT_TIMEOUT_MS } from './constants.js';

// =============================================================================
// Re-exports for backward compatibility
// =============================================================================

export { LakeError, ConnectionError, QueryError } from './errors.js';

/**
 * Re-export CDCStreamController for backward compatibility.
 * The actual implementation is in the cdc-stream module.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export {
  CDCStreamController,
  type CDCStreamControllerOptions,
  type CDCStreamMetrics,
  type BackpressureStrategy,
  type WaterMarkEvent,
} from './cdc-stream/index.js';

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
 * This implementation uses a facade pattern, composing:
 * - {@link WebSocketConnectionManager} for connection lifecycle
 * - {@link QueryExecutor} for RPC operations and response transformation
 * - {@link CDCStreamController} for CDC streaming with backpressure
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
  private readonly connectionManager: WebSocketConnectionManager;
  private readonly queryExecutor: QueryExecutor;
  private cdcStream: CDCStreamController | null = null;

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
      timeout: config.timeout ?? DEFAULT_TIMEOUT_MS,
      retry: config.retry ?? DEFAULT_RETRY_CONFIG,
    };

    // Initialize connection manager
    this.connectionManager = new WebSocketConnectionManager({
      url: this.config.url,
      token: this.config.token,
      timeout: this.config.timeout,
      retry: this.config.retry,
    });

    // Initialize query executor
    this.queryExecutor = new QueryExecutor({
      timeout: this.config.timeout,
    });

    // Wire up message handling
    this.connectionManager.setMessageHandler((data) => this.handleMessage(data));
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
    return this.connectionManager.isConnected;
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
    this.connectionManager.on(event, handler as ConnectionEventHandler);
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
    this.connectionManager.off(event, handler as ConnectionEventHandler);
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
    this.connectionManager.once(event, handler as ConnectionEventHandler);
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
  // Connection Management (Private)
  // ===========================================================================

  private async ensureConnection(): Promise<void> {
    if (!this.connectionManager.isConnected) {
      await this.connectionManager.connect();
      // Set up the RPC sender once connected
      this.queryExecutor.setSender((request) => {
        this.connectionManager.send(JSON.stringify(request));
      });
    }
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
      this.queryExecutor.handleResponse(parsed as LakeRPCResponse);
    } catch (error) {
      console.error('Failed to parse RPC response:', error);
    }
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
    await this.ensureConnection();
    return this.queryExecutor.query<T>(sql, options);
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
    await this.queryExecutor.subscribe(options ?? {});

    // Create stream controller
    this.cdcStream = new CDCStreamController();

    try {
      yield* this.cdcStream;
    } finally {
      // Unsubscribe when done
      await this.queryExecutor.unsubscribe();
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
    await this.ensureConnection();
    return this.queryExecutor.getMetadata(tableName);
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
    await this.ensureConnection();
    return this.queryExecutor.listPartitions(tableName);
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
    await this.ensureConnection();
    return this.queryExecutor.compact(partition, config);
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
    await this.ensureConnection();
    return this.queryExecutor.getCompactionStatus(jobId);
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
    await this.ensureConnection();
    return this.queryExecutor.listSnapshots(tableName);
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
    await this.ensureConnection();
    const start = performance.now();
    await this.queryExecutor.ping();
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
    // Close CDC stream if active
    if (this.cdcStream) {
      this.cdcStream.close();
      this.cdcStream = null;
    }

    // Cancel any pending requests
    this.queryExecutor.cancelAllRequests(
      new ConnectionError({
        code: 'CONNECTION_CLOSED',
        message: 'Connection closed',
      }, this.config.url)
    );

    // Close the connection
    await this.connectionManager.close();
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
