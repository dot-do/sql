/**
 * @dotdo/lake.do - CapnWeb RPC Client for DoLake
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
  CDCEvent,
} from './types.js';
import { createPartitionKey, createCompactionJobId, createSnapshotId } from './types.js';

// =============================================================================
// Client Configuration
// =============================================================================

export interface LakeClientConfig {
  /** DoLake endpoint URL */
  url: string;
  /** Authentication token */
  token?: string;
  /** Request timeout in milliseconds */
  timeout?: number;
  /** Retry configuration */
  retry?: RetryConfig;
}

export interface RetryConfig {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

const DEFAULT_RETRY: RetryConfig = {
  maxRetries: 3,
  baseDelayMs: 100,
  maxDelayMs: 5000,
};

// =============================================================================
// CapnWeb RPC Client
// =============================================================================

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
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return this.ws;
    }

    return new Promise((resolve, reject) => {
      const wsUrl = this.config.url.replace(/^http/, 'ws');
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        resolve(this.ws!);
      };

      this.ws.onerror = (event) => {
        reject(new Error(`WebSocket error: ${event}`));
      };

      this.ws.onclose = () => {
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
      };

      this.ws.onmessage = (event) => {
        this.handleMessage(event.data);
      };
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

  async query<T = Record<string, unknown>>(
    sql: string,
    options?: LakeQueryOptions
  ): Promise<LakeQueryResult<T>> {
    return this.rpc<LakeQueryResult<T>>('query', { sql, ...options });
  }

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

  async getMetadata(tableName: string): Promise<TableMetadata> {
    const result = await this.rpc<TableMetadata>('getMetadata', { tableName });
    // Convert snapshot IDs
    return {
      ...result,
      currentSnapshotId: result.currentSnapshotId
        ? createSnapshotId(result.currentSnapshotId as unknown as string)
        : undefined,
      snapshots: result.snapshots.map((s) => ({
        ...s,
        id: createSnapshotId(s.id as unknown as string),
        parentId: s.parentId ? createSnapshotId(s.parentId as unknown as string) : undefined,
        timestamp: new Date(s.timestamp),
      })),
    };
  }

  async listPartitions(tableName: string): Promise<PartitionInfo[]> {
    const result = await this.rpc<PartitionInfo[]>('listPartitions', { tableName });
    return result.map((p) => ({
      ...p,
      key: createPartitionKey(p.key as unknown as string),
      lastModified: new Date(p.lastModified),
    }));
  }

  async compact(
    partition: PartitionKey,
    config?: Partial<CompactionConfig>
  ): Promise<CompactionJob> {
    const result = await this.rpc<CompactionJob>('compact', { partition, config });
    return this.transformCompactionJob(result);
  }

  async getCompactionStatus(jobId: CompactionJobId): Promise<CompactionJob> {
    const result = await this.rpc<CompactionJob>('getCompactionStatus', { jobId });
    return this.transformCompactionJob(result);
  }

  private transformCompactionJob(job: CompactionJob): CompactionJob {
    return {
      ...job,
      id: createCompactionJobId(job.id as unknown as string),
      partition: createPartitionKey(job.partition as unknown as string),
      startedAt: job.startedAt ? new Date(job.startedAt) : undefined,
      completedAt: job.completedAt ? new Date(job.completedAt) : undefined,
    };
  }

  async listSnapshots(tableName: string): Promise<Snapshot[]> {
    const result = await this.rpc<Snapshot[]>('listSnapshots', { tableName });
    return result.map((s) => ({
      ...s,
      id: createSnapshotId(s.id as unknown as string),
      parentId: s.parentId ? createSnapshotId(s.parentId as unknown as string) : undefined,
      timestamp: new Date(s.timestamp),
    }));
  }

  async ping(): Promise<{ latency: number }> {
    const start = performance.now();
    await this.rpc<void>('ping', {});
    return { latency: performance.now() - start };
  }

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

class CDCStreamController implements AsyncIterable<CDCBatch> {
  private queue: CDCBatch[] = [];
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

export class LakeError extends Error {
  readonly code: string;
  readonly details?: unknown;

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

export function createLakeClient(config: LakeClientConfig): LakeClient {
  return new DoLakeClient(config);
}
