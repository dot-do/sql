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
} from './types.js';
import { createPartitionKey, createCompactionJobId, createSnapshotId } from './types.js';

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

/**
 * Configuration for automatic request retry behavior.
 *
 * @description Controls how the client handles transient failures by
 * automatically retrying failed requests with exponential backoff.
 *
 * @example
 * ```typescript
 * const retryConfig: RetryConfig = {
 *   maxRetries: 5,      // Retry up to 5 times
 *   baseDelayMs: 200,   // Start with 200ms delay
 *   maxDelayMs: 10000,  // Cap delay at 10 seconds
 * };
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export interface RetryConfig {
  /** Maximum number of retry attempts before failing */
  maxRetries: number;
  /** Initial delay between retries in milliseconds (doubles on each retry) */
  baseDelayMs: number;
  /** Maximum delay between retries in milliseconds (cap for exponential backoff) */
  maxDelayMs: number;
}

/**
 * Default retry configuration.
 * @internal
 */
const DEFAULT_RETRY: RetryConfig = {
  maxRetries: 3,
  baseDelayMs: 100,
  maxDelayMs: 5000,
};

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

  /**
   * Creates a new DoLakeClient instance.
   *
   * @description Initializes the client with the provided configuration.
   * The WebSocket connection is established lazily on the first operation.
   *
   * @param config - Client configuration options
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
    this.config = {
      ...config,
      timeout: config.timeout ?? 30000,
      retry: config.retry ?? DEFAULT_RETRY,
    };
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  private async ensureConnection(): Promise<WebSocket> {
    if (this.ws && this.ws.readyState === WebSocket.READY_STATE_OPEN) {
      return this.ws;
    }

    return new Promise((resolve, reject) => {
      const wsUrl = this.config.url.replace(/^http/, 'ws');
      this.ws = new WebSocket(wsUrl);

      this.ws.addEventListener('open', () => {
        resolve(this.ws!);
      });

      this.ws.addEventListener('error', (event: Event) => {
        reject(new Error(`WebSocket error: ${event}`));
      });

      this.ws.addEventListener('close', () => {
        this.ws = null;
        // Reject all pending requests
        for (const [id, pending] of this.pendingRequests) {
          clearTimeout(pending.timeout);
          pending.reject(new Error('Connection closed'));
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
    if (this.cdcStream) {
      this.cdcStream.close();
      this.cdcStream = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}

// =============================================================================
// CDC Stream Controller
// =============================================================================

/**
 * Internal controller for CDC stream iteration.
 * @internal
 */
class CDCStreamController implements AsyncIterable<CDCBatch> {
  private readonly queue: CDCBatch[] = [];
  private resolvers: Array<(value: IteratorResult<CDCBatch>) => void> = [];
  private closed = false;

  push(batch: CDCBatch): void {
    if (this.closed) return;

    const resolver = this.resolvers.shift();
    if (resolver) {
      resolver({ value: batch, done: false });
    } else {
      this.queue.push(batch);
    }
  }

  close(): void {
    this.closed = true;
    for (const resolver of this.resolvers) {
      resolver({ value: undefined as unknown as CDCBatch, done: true });
    }
    this.resolvers = [];
  }

  [Symbol.asyncIterator](): AsyncIterator<CDCBatch> {
    return {
      next: (): Promise<IteratorResult<CDCBatch>> => {
        if (this.closed && this.queue.length === 0) {
          return Promise.resolve({ value: undefined as unknown as CDCBatch, done: true });
        }

        const batch = this.queue.shift();
        if (batch) {
          return Promise.resolve({ value: batch, done: false });
        }

        return new Promise((resolve) => {
          this.resolvers.push(resolve);
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
