/**
 * DoLake E2E Test Client
 *
 * HTTP and WebSocket client utilities for E2E testing of the lake.do package.
 * Provides a simple interface for testing DoLake endpoints.
 *
 * @packageDocumentation
 */

import type {
  LakeQueryResult,
  CDCBatch,
  CDCStreamOptions,
  TableMetadata,
  PartitionInfo,
  Snapshot,
  CompactionJob,
  PartitionKey,
  CompactionJobId,
} from '../src/types.js';

// =============================================================================
// Types
// =============================================================================

export interface E2EClientConfig {
  /** Base URL of the DoLake worker */
  baseUrl: string;
  /** Lakehouse name for this test run */
  lakehouseName?: string;
  /** Request timeout in milliseconds */
  timeoutMs?: number;
  /** Whether to log requests/responses */
  debug?: boolean;
  /** Authentication token */
  token?: string;
}

export interface HealthResponse {
  status: string;
  service?: string;
  version?: string;
  initialized?: boolean;
}

export interface CDCWriteResult {
  success: boolean;
  eventsWritten: number;
  sequenceNumber: number;
  error?: string;
}

export interface CDCEvent {
  id: string;
  table: string;
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  timestamp: number;
  lsn: bigint;
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
}

export interface LatencyMeasurement {
  p50: number;
  p95: number;
  p99: number;
  min: number;
  max: number;
  avg: number;
  count: number;
  samples: number[];
}

// =============================================================================
// HTTP Client for DoLake E2E Testing
// =============================================================================

/**
 * E2E HTTP client for DoLake
 */
export class DoLakeE2EClient {
  private config: Required<Omit<E2EClientConfig, 'token'>> & { token?: string };

  constructor(config: E2EClientConfig) {
    this.config = {
      timeoutMs: 30_000,
      debug: false,
      lakehouseName: 'test',
      ...config,
    };
  }

  /**
   * Get the full URL for a lakehouse endpoint
   */
  private getLakeUrl(path: string): string {
    return `${this.config.baseUrl}/lake/${this.config.lakehouseName}${path}`;
  }

  /**
   * Make an HTTP request with timeout and error handling
   */
  private async request<T>(
    url: string,
    options: RequestInit = {}
  ): Promise<T> {
    const controller = new AbortController();
    const timeoutId = setTimeout(
      () => controller.abort(),
      this.config.timeoutMs
    );

    const startTime = performance.now();

    try {
      if (this.config.debug) {
        console.log(`[DoLake E2E] ${options.method || 'GET'} ${url}`);
        if (options.body) {
          console.log(`[DoLake E2E] Body: ${options.body}`);
        }
      }

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        ...(options.headers as Record<string, string>),
      };

      if (this.config.token) {
        headers['Authorization'] = `Bearer ${this.config.token}`;
      }

      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
        headers,
      });

      const data = await response.json() as T;

      if (this.config.debug) {
        const elapsed = performance.now() - startTime;
        console.log(`[DoLake E2E] Response (${elapsed.toFixed(2)}ms):`, data);
      }

      return data;
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error(`Request timeout after ${this.config.timeoutMs}ms: ${url}`);
      }
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  // ===========================================================================
  // Health & Status
  // ===========================================================================

  /**
   * Check worker health
   */
  async health(): Promise<HealthResponse> {
    return this.request<HealthResponse>(`${this.config.baseUrl}/health`);
  }

  /**
   * Check lakehouse health
   */
  async lakehouseHealth(): Promise<HealthResponse> {
    return this.request<HealthResponse>(this.getLakeUrl('/health'));
  }

  /**
   * Wait for the lakehouse to be ready
   */
  async waitForReady(timeoutMs = 30_000): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeoutMs) {
      try {
        const health = await this.lakehouseHealth();
        if (health.status === 'ok') {
          return;
        }
      } catch {
        // Continue waiting
      }

      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    throw new Error(`Lakehouse not ready after ${timeoutMs}ms`);
  }

  /**
   * Ping the lakehouse and measure latency
   */
  async ping(): Promise<{ latency: number }> {
    const start = performance.now();
    await this.request<void>(this.getLakeUrl('/ping'), { method: 'POST' });
    return { latency: performance.now() - start };
  }

  // ===========================================================================
  // CDC Operations
  // ===========================================================================

  /**
   * Write a CDC batch to the lakehouse
   */
  async writeCDCBatch(events: CDCEvent[]): Promise<CDCWriteResult> {
    return this.request<CDCWriteResult>(this.getLakeUrl('/cdc/write'), {
      method: 'POST',
      body: JSON.stringify({ events }),
    });
  }

  /**
   * Acknowledge CDC events up to a sequence number
   */
  async ackCDC(sequenceNumber: number): Promise<{ success: boolean }> {
    return this.request<{ success: boolean }>(this.getLakeUrl('/cdc/ack'), {
      method: 'POST',
      body: JSON.stringify({ sequenceNumber }),
    });
  }

  /**
   * Get CDC stream state
   */
  async getCDCState(): Promise<{
    lastSequenceNumber: number;
    lastAckedSequenceNumber: number;
    pendingEvents: number;
  }> {
    return this.request(this.getLakeUrl('/cdc/state'));
  }

  // ===========================================================================
  // Query Operations
  // ===========================================================================

  /**
   * Execute a SQL query against the lakehouse
   */
  async query<T = Record<string, unknown>>(
    sql: string,
    options: { asOf?: Date | string; partitions?: string[] } = {}
  ): Promise<LakeQueryResult<T>> {
    return this.request<LakeQueryResult<T>>(this.getLakeUrl('/query'), {
      method: 'POST',
      body: JSON.stringify({
        sql,
        asOf: options.asOf instanceof Date ? options.asOf.toISOString() : options.asOf,
        partitions: options.partitions,
      }),
    });
  }

  // ===========================================================================
  // Metadata Operations
  // ===========================================================================

  /**
   * Get table metadata
   */
  async getMetadata(tableName: string): Promise<TableMetadata> {
    return this.request<TableMetadata>(this.getLakeUrl(`/tables/${tableName}/metadata`));
  }

  /**
   * List all tables
   */
  async listTables(): Promise<string[]> {
    const result = await this.request<{ tables: string[] }>(this.getLakeUrl('/tables'));
    return result.tables;
  }

  /**
   * List partitions for a table
   */
  async listPartitions(tableName: string): Promise<PartitionInfo[]> {
    return this.request<PartitionInfo[]>(this.getLakeUrl(`/tables/${tableName}/partitions`));
  }

  /**
   * List snapshots for a table
   */
  async listSnapshots(tableName: string): Promise<Snapshot[]> {
    return this.request<Snapshot[]>(this.getLakeUrl(`/tables/${tableName}/snapshots`));
  }

  // ===========================================================================
  // Compaction Operations
  // ===========================================================================

  /**
   * Trigger compaction for a partition
   */
  async compact(
    tableName: string,
    partitionKey: string,
    config?: { targetFileSize?: number; maxFiles?: number }
  ): Promise<CompactionJob> {
    return this.request<CompactionJob>(
      this.getLakeUrl(`/tables/${tableName}/partitions/${encodeURIComponent(partitionKey)}/compact`),
      {
        method: 'POST',
        body: JSON.stringify(config || {}),
      }
    );
  }

  /**
   * Get compaction job status
   */
  async getCompactionStatus(jobId: string): Promise<CompactionJob> {
    return this.request<CompactionJob>(this.getLakeUrl(`/compaction/${jobId}`));
  }

  // ===========================================================================
  // Performance Measurement
  // ===========================================================================

  /**
   * Measure latency for an operation
   */
  async measureLatency(
    operation: () => Promise<unknown>,
    iterations = 100
  ): Promise<LatencyMeasurement> {
    const samples: number[] = [];

    for (let i = 0; i < iterations; i++) {
      const start = performance.now();
      await operation();
      const elapsed = performance.now() - start;
      samples.push(elapsed);
    }

    // Sort for percentile calculation
    samples.sort((a, b) => a - b);

    return {
      p50: this.percentile(samples, 50),
      p95: this.percentile(samples, 95),
      p99: this.percentile(samples, 99),
      min: samples[0],
      max: samples[samples.length - 1],
      avg: samples.reduce((a, b) => a + b, 0) / samples.length,
      count: samples.length,
      samples,
    };
  }

  private percentile(sorted: number[], p: number): number {
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.max(0, index)];
  }

  /**
   * Measure cold start latency
   */
  async measureColdStart(iterations = 5): Promise<LatencyMeasurement> {
    const samples: number[] = [];

    for (let i = 0; i < iterations; i++) {
      // Use a unique lakehouse name to ensure cold start
      const uniqueName = `cold-${Date.now()}-${i}`;
      const url = `${this.config.baseUrl}/lake/${uniqueName}/health`;

      const start = performance.now();
      await fetch(url);
      const elapsed = performance.now() - start;
      samples.push(elapsed);

      // Wait a bit between iterations
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    samples.sort((a, b) => a - b);

    return {
      p50: this.percentile(samples, 50),
      p95: this.percentile(samples, 95),
      p99: this.percentile(samples, 99),
      min: samples[0],
      max: samples[samples.length - 1],
      avg: samples.reduce((a, b) => a + b, 0) / samples.length,
      count: samples.length,
      samples,
    };
  }
}

// =============================================================================
// WebSocket Client for CDC Streaming
// =============================================================================

/**
 * WebSocket client for CDC streaming
 */
export class DoLakeWebSocketClient {
  private config: Required<Omit<E2EClientConfig, 'token'>> & { token?: string };
  private ws: WebSocket | null = null;
  private messageQueue: CDCBatch[] = [];
  private waitingResolvers: Array<(batch: CDCBatch) => void> = [];
  private connected = false;

  constructor(config: E2EClientConfig) {
    this.config = {
      timeoutMs: 30_000,
      debug: false,
      lakehouseName: 'test',
      ...config,
    };
  }

  /**
   * Connect to the WebSocket endpoint
   */
  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const wsUrl = this.config.baseUrl
        .replace('https://', 'wss://')
        .replace('http://', 'ws://');
      const fullUrl = `${wsUrl}/lake/${this.config.lakehouseName}/ws`;

      if (this.config.debug) {
        console.log(`[DoLake WS] Connecting to ${fullUrl}`);
      }

      this.ws = new WebSocket(fullUrl);

      const timeout = setTimeout(() => {
        reject(new Error(`WebSocket connection timeout after ${this.config.timeoutMs}ms`));
      }, this.config.timeoutMs);

      this.ws.onopen = () => {
        clearTimeout(timeout);
        this.connected = true;
        if (this.config.debug) {
          console.log('[DoLake WS] Connected');
        }

        // Send auth if token provided
        if (this.config.token && this.ws) {
          this.ws.send(JSON.stringify({ type: 'auth', token: this.config.token }));
        }

        resolve();
      };

      this.ws.onerror = (error) => {
        clearTimeout(timeout);
        reject(new Error(`WebSocket error: ${error}`));
      };

      this.ws.onclose = () => {
        this.connected = false;
        if (this.config.debug) {
          console.log('[DoLake WS] Disconnected');
        }
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string) as CDCBatch;
          if (this.config.debug) {
            console.log('[DoLake WS] Received:', data);
          }

          // If someone is waiting for a batch, deliver it
          const resolver = this.waitingResolvers.shift();
          if (resolver) {
            resolver(data);
          } else {
            this.messageQueue.push(data);
          }
        } catch (error) {
          console.error('[DoLake WS] Failed to parse message:', error);
        }
      };
    });
  }

  /**
   * Subscribe to CDC events
   */
  async subscribe(options: CDCStreamOptions = {}): Promise<void> {
    if (!this.ws || !this.connected) {
      throw new Error('WebSocket not connected');
    }

    const message = {
      type: 'subscribe',
      ...options,
    };

    this.ws.send(JSON.stringify(message));
  }

  /**
   * Unsubscribe from CDC events
   */
  async unsubscribe(): Promise<void> {
    if (!this.ws || !this.connected) {
      return;
    }

    this.ws.send(JSON.stringify({ type: 'unsubscribe' }));
  }

  /**
   * Wait for the next CDC batch
   */
  async nextBatch(timeoutMs = 10_000): Promise<CDCBatch> {
    // Check queue first
    const queued = this.messageQueue.shift();
    if (queued) {
      return queued;
    }

    // Wait for next batch
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this.waitingResolvers.indexOf(resolve);
        if (index >= 0) {
          this.waitingResolvers.splice(index, 1);
        }
        reject(new Error(`Timeout waiting for CDC batch after ${timeoutMs}ms`));
      }, timeoutMs);

      this.waitingResolvers.push((batch) => {
        clearTimeout(timeout);
        resolve(batch);
      });
    });
  }

  /**
   * Collect batches for a duration
   */
  async collectBatches(durationMs: number): Promise<CDCBatch[]> {
    const batches: CDCBatch[] = [...this.messageQueue];
    this.messageQueue.length = 0;

    const endTime = Date.now() + durationMs;

    while (Date.now() < endTime) {
      try {
        const batch = await this.nextBatch(Math.max(100, endTime - Date.now()));
        batches.push(batch);
      } catch {
        // Timeout, continue
      }
    }

    return batches;
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Close the connection
   */
  close(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.connected = false;
    this.messageQueue.length = 0;
    this.waitingResolvers.length = 0;
  }
}

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a new E2E client for a test lakehouse
 */
export function createTestClient(
  baseUrl: string,
  testName: string,
  options: { debug?: boolean; token?: string } = {}
): DoLakeE2EClient {
  const lakehouseName = `test-${testName}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
  return new DoLakeE2EClient({
    baseUrl,
    lakehouseName,
    debug: options.debug ?? false,
    token: options.token,
  });
}

/**
 * Retry an operation with exponential backoff
 */
export async function retry<T>(
  operation: () => Promise<T>,
  options: {
    maxAttempts?: number;
    initialDelayMs?: number;
    maxDelayMs?: number;
    retryOn?: (error: unknown) => boolean;
  } = {}
): Promise<T> {
  const {
    maxAttempts = 3,
    initialDelayMs = 100,
    maxDelayMs = 5000,
    retryOn = () => true,
  } = options;

  let lastError: unknown;
  let delay = initialDelayMs;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;

      if (attempt === maxAttempts || !retryOn(error)) {
        throw error;
      }

      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, maxDelayMs);
    }
  }

  throw lastError;
}

/**
 * Generate mock CDC events for testing
 */
export function generateCDCEvents(
  table: string,
  count: number,
  operation: 'INSERT' | 'UPDATE' | 'DELETE' = 'INSERT'
): CDCEvent[] {
  const events: CDCEvent[] = [];
  const baseTimestamp = Date.now();

  for (let i = 0; i < count; i++) {
    const event: CDCEvent = {
      id: `evt_${baseTimestamp}_${i}`,
      table,
      operation,
      timestamp: baseTimestamp + i,
      lsn: BigInt(baseTimestamp + i),
      after: operation !== 'DELETE' ? { id: i + 1, value: `value_${i + 1}` } : undefined,
      before: operation !== 'INSERT' ? { id: i + 1, value: `old_value_${i + 1}` } : undefined,
    };
    events.push(event);
  }

  return events;
}

// Note: Types are exported inline with their definitions above
