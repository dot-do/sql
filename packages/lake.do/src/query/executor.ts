/**
 * @dotdo/lake.do - Query Executor
 *
 * This module provides query execution and response transformation logic.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import type {
  LakeQueryOptions,
  LakeQueryResult,
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
} from '../types.js';
import {
  createPartitionKey,
  createCompactionJobId,
  createSnapshotId,
} from '../types.js';
import { LakeError, TimeoutError } from '../errors.js';

// =============================================================================
// Types
// =============================================================================

/**
 * RPC sender function type for sending requests.
 *
 * @internal
 */
export type RPCSender = (request: LakeRPCRequest) => void;

/**
 * Request handler for RPC responses.
 *
 * @internal
 */
export interface RequestHandler {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
}

/**
 * Configuration for QueryExecutor.
 *
 * @internal
 */
export interface QueryExecutorConfig {
  /** Request timeout in milliseconds */
  timeout: number;
}

// =============================================================================
// QueryExecutor Implementation
// =============================================================================

/**
 * Handles query execution and response transformations.
 *
 * @description Separates RPC invocation logic and response transformation
 * from the main client class. Handles timeouts, error conversion, and
 * type-safe response parsing.
 *
 * @example
 * ```typescript
 * const executor = new QueryExecutor({
 *   timeout: 30000,
 * });
 *
 * // Set up sender and handlers
 * executor.setSender((request) => ws.send(JSON.stringify(request)));
 *
 * // Execute a query
 * const result = await executor.query<Order>('SELECT * FROM orders', {});
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class QueryExecutor {
  private readonly config: QueryExecutorConfig;
  private requestId = 0;
  private sender: RPCSender | null = null;
  private readonly pendingRequests = new Map<string, RequestHandler>();

  /**
   * Creates a new QueryExecutor.
   *
   * @param config - Executor configuration
   */
  constructor(config: QueryExecutorConfig) {
    this.config = config;
  }

  // ===========================================================================
  // Configuration
  // ===========================================================================

  /**
   * Sets the RPC sender function.
   *
   * @param sender - Function to send RPC requests
   */
  setSender(sender: RPCSender): void {
    this.sender = sender;
  }

  /**
   * Returns the number of pending requests.
   */
  get pendingRequestCount(): number {
    return this.pendingRequests.size;
  }

  // ===========================================================================
  // Response Handling
  // ===========================================================================

  /**
   * Handles an RPC response.
   *
   * @param response - The parsed RPC response
   */
  handleResponse(response: LakeRPCResponse): void {
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
  }

  /**
   * Cancels all pending requests with the given error.
   *
   * @param error - The error to reject with
   */
  cancelAllRequests(error: Error): void {
    for (const [id, pending] of this.pendingRequests) {
      clearTimeout(pending.timeout);
      pending.reject(error);
      this.pendingRequests.delete(id);
    }
  }

  // ===========================================================================
  // RPC Methods
  // ===========================================================================

  /**
   * Sends an RPC request and waits for a response.
   *
   * @param method - The RPC method to call
   * @param params - The method parameters
   * @returns Promise resolving to the response
   * @throws {Error} When sender is not configured
   * @throws {TimeoutError} When request times out
   * @throws {LakeError} When server returns an error
   */
  async rpc<T>(method: string, params: unknown): Promise<T> {
    if (!this.sender) {
      throw new Error('RPC sender not configured');
    }

    const id = `${++this.requestId}`;

    const request: LakeRPCRequest = {
      id,
      method: method as LakeRPCRequest['method'],
      params,
    };

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new TimeoutError(method));
      }, this.config.timeout);

      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeout,
      });

      this.sender!(request);
    });
  }

  // ===========================================================================
  // Query Operations
  // ===========================================================================

  /**
   * Executes a SQL query.
   *
   * @param sql - The SQL query string
   * @param options - Query options
   * @returns Query result with rows and statistics
   */
  async query<T = Record<string, unknown>>(
    sql: string,
    options?: LakeQueryOptions
  ): Promise<LakeQueryResult<T>> {
    return this.rpc<LakeQueryResult<T>>('query', { sql, ...options });
  }

  // ===========================================================================
  // Metadata Operations
  // ===========================================================================

  /**
   * Retrieves metadata for a table.
   *
   * @param tableName - The table name
   * @returns Table metadata with transformed snapshot IDs
   */
  async getMetadata(tableName: string): Promise<TableMetadata> {
    const result = await this.rpc<TableMetadata>('getMetadata', { tableName });
    return this.transformTableMetadata(result);
  }

  /**
   * Lists all snapshots for a table.
   *
   * @param tableName - The table name
   * @returns Array of transformed snapshots
   */
  async listSnapshots(tableName: string): Promise<Snapshot[]> {
    const result = await this.rpc<Snapshot[]>('listSnapshots', { tableName });
    return result.map((s) => this.transformSnapshot(s));
  }

  // ===========================================================================
  // Partition Operations
  // ===========================================================================

  /**
   * Lists all partitions for a table.
   *
   * @param tableName - The table name
   * @returns Array of partition info with transformed keys
   */
  async listPartitions(tableName: string): Promise<PartitionInfo[]> {
    const result = await this.rpc<PartitionInfo[]>('listPartitions', { tableName });
    return result.map((p) => ({
      ...p,
      key: createPartitionKey(p.key as unknown as string),
      lastModified: new Date(p.lastModified),
    }));
  }

  // ===========================================================================
  // Compaction Operations
  // ===========================================================================

  /**
   * Triggers a compaction job for a partition.
   *
   * @param partition - The partition key
   * @param config - Optional compaction configuration
   * @returns The created compaction job
   */
  async compact(
    partition: PartitionKey,
    config?: Partial<CompactionConfig>
  ): Promise<CompactionJob> {
    const result = await this.rpc<CompactionJob>('compact', { partition, config });
    return this.transformCompactionJob(result);
  }

  /**
   * Gets the status of a compaction job.
   *
   * @param jobId - The compaction job ID
   * @returns Current job status
   */
  async getCompactionStatus(jobId: CompactionJobId): Promise<CompactionJob> {
    const result = await this.rpc<CompactionJob>('getCompactionStatus', { jobId });
    return this.transformCompactionJob(result);
  }

  // ===========================================================================
  // Utility Operations
  // ===========================================================================

  /**
   * Subscribes to CDC stream.
   *
   * @param options - Subscription options
   */
  async subscribe(options: unknown): Promise<void> {
    await this.rpc<void>('subscribe', options ?? {});
  }

  /**
   * Unsubscribes from CDC stream.
   */
  async unsubscribe(): Promise<void> {
    await this.rpc<void>('unsubscribe', {}).catch(() => {});
  }

  /**
   * Sends a ping request.
   */
  async ping(): Promise<void> {
    await this.rpc<void>('ping', {});
  }

  // ===========================================================================
  // Response Transformers
  // ===========================================================================

  /**
   * Transforms table metadata response.
   */
  private transformTableMetadata(result: TableMetadata): TableMetadata {
    const metadata: TableMetadata = {
      tableId: result.tableId,
      schema: result.schema,
      partitionSpec: result.partitionSpec,
      properties: result.properties,
      snapshots: result.snapshots.map((s) => this.transformSnapshot(s)),
    };
    if (result.currentSnapshotId) {
      metadata.currentSnapshotId = createSnapshotId(result.currentSnapshotId as unknown as string);
    }
    return metadata;
  }

  /**
   * Transforms a snapshot response.
   */
  private transformSnapshot(s: Snapshot): Snapshot {
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
  }

  /**
   * Transforms a compaction job response.
   */
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
}
