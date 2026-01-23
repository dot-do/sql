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

/**
 * E2E-specific CDCEvent type for DoLake testing.
 *
 * NOTE: This differs from the canonical CDCEvent in sql.do:
 * - Includes `id` field (E2E-specific event identifier for test assertions)
 * - Operation is a simple union rather than the CDCOperation branded type
 * - Missing optional fields like `transactionId`, `sequence`, `metadata`
 * This is intentional - E2E tests work with the external API contract which
 * exposes a simplified event structure for testing purposes.
 *
 * @see sql.do for the canonical CDCEvent interface
 */
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

  /**
   * Creates a new DoLakeE2EClient instance
   * @param config - Client configuration options
   */
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
   * @param path - The endpoint path to append to the base lakehouse URL
   * @returns The complete URL for the lakehouse endpoint
   */
  private getLakeUrl(path: string): string {
    return `${this.config.baseUrl}/lake/${this.config.lakehouseName}${path}`;
  }

  /**
   * Make an HTTP request with timeout and error handling
   * @param url - The full URL to make the request to
   * @param options - Fetch request options (method, body, headers, etc.)
   * @returns The parsed JSON response from the server
   *
   * @throws {Error} When request times out after configured timeoutMs
   * @throws {Error} When network request fails (fetch error)
   * @throws {Error} When response JSON parsing fails
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
   * @returns Health status of the worker including service name, version, and initialization state
   *
   * @throws {Error} When request times out or network fails
   */
  async health(): Promise<HealthResponse> {
    return this.request<HealthResponse>(`${this.config.baseUrl}/health`);
  }

  /**
   * Check lakehouse health
   * @returns Health status of the lakehouse including service name, version, and initialization state
   *
   * @throws {Error} When request times out or network fails
   */
  async lakehouseHealth(): Promise<HealthResponse> {
    return this.request<HealthResponse>(this.getLakeUrl('/health'));
  }

  /**
   * Wait for the lakehouse to be ready
   * @param timeoutMs - Maximum time to wait in milliseconds (default: 30000)
   * @returns Resolves when the lakehouse is ready
   *
   * @throws {Error} When lakehouse is not ready after timeoutMs elapsed
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
   * @returns Object containing the round-trip latency in milliseconds
   *
   * @throws {Error} When request times out or network fails
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
   * @param events - Array of CDC events to write
   * @returns Result of the write operation including success status, events written count, and sequence number
   *
   * @throws {Error} When request times out or network fails
   * @throws {Error} When server returns an error response
   */
  async writeCDCBatch(events: CDCEvent[]): Promise<CDCWriteResult> {
    return this.request<CDCWriteResult>(this.getLakeUrl('/cdc/write'), {
      method: 'POST',
      body: JSON.stringify({ events }),
    });
  }

  /**
   * Acknowledge CDC events up to a sequence number
   * @param sequenceNumber - The sequence number up to which events are acknowledged
   * @returns Object indicating whether the acknowledgment was successful
   *
   * @throws {Error} When request times out or network fails
   */
  async ackCDC(sequenceNumber: number): Promise<{ success: boolean }> {
    return this.request<{ success: boolean }>(this.getLakeUrl('/cdc/ack'), {
      method: 'POST',
      body: JSON.stringify({ sequenceNumber }),
    });
  }

  /**
   * Get CDC stream state
   * @returns Current CDC stream state including last sequence number, last acked sequence number, and pending events count
   *
   * @throws {Error} When request times out or network fails
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
   * @param sql - The SQL query string to execute
   * @param options - Query options including time travel (asOf) and partition filtering
   * @returns Query result containing rows of type T and metadata
   *
   * @throws {Error} When request times out or network fails
   * @throws {Error} When SQL query is invalid or execution fails
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
   * @param tableName - The name of the table to get metadata for
   * @returns Metadata for the specified table including schema, partitioning, and properties
   *
   * @throws {Error} When request times out or network fails
   * @throws {Error} When table does not exist
   */
  async getMetadata(tableName: string): Promise<TableMetadata> {
    return this.request<TableMetadata>(this.getLakeUrl(`/tables/${tableName}/metadata`));
  }

  /**
   * List all tables
   * @returns Array of table names in the lakehouse
   *
   * @throws {Error} When request times out or network fails
   */
  async listTables(): Promise<string[]> {
    const result = await this.request<{ tables: string[] }>(this.getLakeUrl('/tables'));
    return result.tables;
  }

  /**
   * List partitions for a table
   * @param tableName - The name of the table to list partitions for
   * @returns Array of partition information for the specified table
   *
   * @throws {Error} When request times out or network fails
   * @throws {Error} When table does not exist
   */
  async listPartitions(tableName: string): Promise<PartitionInfo[]> {
    return this.request<PartitionInfo[]>(this.getLakeUrl(`/tables/${tableName}/partitions`));
  }

  /**
   * List snapshots for a table
   * @param tableName - The name of the table to list snapshots for
   * @returns Array of snapshots for the specified table
   *
   * @throws {Error} When request times out or network fails
   * @throws {Error} When table does not exist
   */
  async listSnapshots(tableName: string): Promise<Snapshot[]> {
    return this.request<Snapshot[]>(this.getLakeUrl(`/tables/${tableName}/snapshots`));
  }

  // ===========================================================================
  // Compaction Operations
  // ===========================================================================

  /**
   * Trigger compaction for a partition
   * @param tableName - The name of the table containing the partition
   * @param partitionKey - The partition key to compact
   * @param config - Optional compaction configuration (target file size, max files)
   * @returns The compaction job that was created
   *
   * @throws {Error} When request times out or network fails
   * @throws {Error} When table or partition does not exist
   * @throws {Error} When compaction job creation fails
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
   * @param jobId - The ID of the compaction job to check
   * @returns Current status of the compaction job
   *
   * @throws {Error} When request times out or network fails
   * @throws {Error} When compaction job does not exist
   */
  async getCompactionStatus(jobId: string): Promise<CompactionJob> {
    return this.request<CompactionJob>(this.getLakeUrl(`/compaction/${jobId}`));
  }

  // ===========================================================================
  // Performance Measurement
  // ===========================================================================

  /**
   * Measure latency for an operation
   * @param operation - The async operation to measure
   * @param iterations - Number of times to run the operation (default: 100)
   * @returns Latency statistics including percentiles (p50, p95, p99), min, max, avg, and all samples
   *
   * @throws {Error} When the provided operation throws an error
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

  /**
   * Calculate a percentile value from a sorted array
   * @param sorted - The sorted array of numbers
   * @param p - The percentile to calculate (0-100)
   * @returns The value at the specified percentile
   */
  private percentile(sorted: number[], p: number): number {
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.max(0, index)];
  }

  /**
   * Measure cold start latency
   * @param iterations - Number of cold start measurements to take (default: 5)
   * @returns Latency statistics for cold starts including percentiles (p50, p95, p99), min, max, avg, and all samples
   *
   * @throws {Error} When network request fails
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

  /**
   * Creates a new DoLakeWebSocketClient instance
   * @param config - Client configuration options
   */
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
   * @returns Resolves when the WebSocket connection is established
   *
   * @throws {Error} When connection times out after configured timeoutMs
   * @throws {Error} When WebSocket connection fails
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
   * @param options - Subscription options for filtering CDC events
   * @returns Resolves when the subscription message has been sent
   *
   * @throws {Error} When WebSocket is not connected
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
   * @returns Resolves when the unsubscription message has been sent (no-op if not connected)
   */
  async unsubscribe(): Promise<void> {
    if (!this.ws || !this.connected) {
      return;
    }

    this.ws.send(JSON.stringify({ type: 'unsubscribe' }));
  }

  /**
   * Wait for the next CDC batch
   * @param timeoutMs - Maximum time to wait for a batch in milliseconds (default: 10000)
   * @returns The next CDC batch received from the stream
   *
   * @throws {Error} When timeout expires before receiving a batch
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
   * @param durationMs - Duration in milliseconds to collect batches
   * @returns Array of CDC batches collected during the specified duration
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
   * @returns True if the WebSocket is currently connected
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Close the connection
   * @returns void
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
 * @param baseUrl - The base URL of the DoLake worker
 * @param testName - A name for this test (used to generate unique lakehouse name)
 * @param options - Optional configuration (debug mode, auth token)
 * @returns A configured DoLakeE2EClient instance with a unique lakehouse name for testing
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
 * @param operation - The async operation to retry
 * @param options - Retry configuration options
 * @param options.maxAttempts - Maximum number of retry attempts (default: 3)
 * @param options.initialDelayMs - Initial delay between retries in milliseconds (default: 100)
 * @param options.maxDelayMs - Maximum delay between retries in milliseconds (default: 5000)
 * @param options.retryOn - Predicate function to determine if error should trigger retry
 * @returns The result of the operation if successful
 *
 * @throws {Error} When max attempts exceeded and operation still fails
 * @throws {Error} When retryOn predicate returns false for an error
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
 * @param table - The table name for the generated events
 * @param count - Number of events to generate
 * @param operation - The CDC operation type (default: 'INSERT')
 * @returns Array of mock CDC events with sequential timestamps and LSN values
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
