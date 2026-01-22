/**
 * DoSQL E2E Test Client
 *
 * HTTP and WebSocket client utilities for E2E testing.
 * Provides a simple interface for testing DoSQL endpoints.
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

export interface E2EClientConfig {
  /** Base URL of the DoSQL worker */
  baseUrl: string;
  /** Database name for this test run */
  dbName: string;
  /** Request timeout in milliseconds */
  timeoutMs?: number;
  /** Whether to log requests/responses */
  debug?: boolean;
}

export interface QueryResult {
  success: boolean;
  rows?: Record<string, unknown>[];
  error?: string;
  stats?: {
    rowsAffected: number;
    executionTimeMs: number;
  };
}

export interface ExecuteResult {
  success: boolean;
  error?: string;
  stats?: {
    rowsAffected: number;
    executionTimeMs: number;
  };
}

export interface HealthResponse {
  status: string;
  service?: string;
  version?: string;
  initialized?: boolean;
}

export interface TableSchema {
  name: string;
  columns: { name: string; type: string }[];
  primaryKey: string;
}

export interface TablesResponse {
  tables: TableSchema[];
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

export interface CDCEvent {
  lsn: bigint;
  table: string;
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  timestamp: number;
  txId: string;
  oldRow?: Record<string, unknown>;
  newRow?: Record<string, unknown>;
}

// =============================================================================
// HTTP Client
// =============================================================================

/**
 * E2E HTTP client for DoSQL
 */
export class DoSQLE2EClient {
  private config: Required<E2EClientConfig>;

  constructor(config: E2EClientConfig) {
    this.config = {
      timeoutMs: 30_000,
      debug: false,
      ...config,
    };
  }

  /**
   * Get the full URL for a database endpoint
   */
  private getDbUrl(path: string): string {
    return `${this.config.baseUrl}/db/${this.config.dbName}${path}`;
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
        console.log(`[E2E Client] ${options.method || 'GET'} ${url}`);
        if (options.body) {
          console.log(`[E2E Client] Body: ${options.body}`);
        }
      }

      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
        headers: {
          'Content-Type': 'application/json',
          ...options.headers,
        },
      });

      const data = await response.json() as T;

      if (this.config.debug) {
        const elapsed = performance.now() - startTime;
        console.log(`[E2E Client] Response (${elapsed.toFixed(2)}ms):`, data);
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
   * Check database health
   */
  async dbHealth(): Promise<HealthResponse> {
    return this.request<HealthResponse>(this.getDbUrl('/health'));
  }

  /**
   * Wait for the database to be ready
   */
  async waitForReady(timeoutMs = 30_000): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeoutMs) {
      try {
        const health = await this.dbHealth();
        if (health.status === 'ok') {
          return;
        }
      } catch {
        // Continue waiting
      }

      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    throw new Error(`Database not ready after ${timeoutMs}ms`);
  }

  // ===========================================================================
  // Query Operations
  // ===========================================================================

  /**
   * Execute a SELECT query
   */
  async query(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<QueryResult> {
    return this.request<QueryResult>(this.getDbUrl('/query'), {
      method: 'POST',
      body: JSON.stringify({ sql, params }),
    });
  }

  /**
   * Execute an INSERT/UPDATE/DELETE statement
   */
  async execute(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<ExecuteResult> {
    return this.request<ExecuteResult>(this.getDbUrl('/execute'), {
      method: 'POST',
      body: JSON.stringify({ sql, params }),
    });
  }

  /**
   * List all tables
   */
  async listTables(): Promise<TablesResponse> {
    return this.request<TablesResponse>(this.getDbUrl('/tables'));
  }

  // ===========================================================================
  // Convenience Methods
  // ===========================================================================

  /**
   * Create a table
   */
  async createTable(
    name: string,
    columns: { name: string; type: string }[],
    primaryKey: string
  ): Promise<ExecuteResult> {
    const columnDefs = columns.map((c) => `${c.name} ${c.type}`).join(', ');
    const sql = `CREATE TABLE ${name} (${columnDefs}, PRIMARY KEY (${primaryKey}))`;
    return this.execute(sql);
  }

  /**
   * Insert a row
   */
  async insert(
    table: string,
    data: Record<string, unknown>
  ): Promise<ExecuteResult> {
    const columns = Object.keys(data).join(', ');
    const values = Object.values(data)
      .map((v) => (typeof v === 'string' ? `'${v}'` : String(v)))
      .join(', ');
    const sql = `INSERT INTO ${table} (${columns}) VALUES (${values})`;
    return this.execute(sql);
  }

  /**
   * Select all rows from a table
   */
  async selectAll(table: string): Promise<QueryResult> {
    return this.query(`SELECT * FROM ${table}`);
  }

  /**
   * Select with a WHERE clause
   */
  async selectWhere(
    table: string,
    where: Record<string, unknown>
  ): Promise<QueryResult> {
    const conditions = Object.entries(where)
      .map(([k, v]) => `${k} = ${typeof v === 'string' ? `'${v}'` : v}`)
      .join(' AND ');
    return this.query(`SELECT * FROM ${table} WHERE ${conditions}`);
  }

  /**
   * Update rows
   */
  async update(
    table: string,
    set: Record<string, unknown>,
    where: Record<string, unknown>
  ): Promise<ExecuteResult> {
    const setClause = Object.entries(set)
      .map(([k, v]) => `${k} = ${typeof v === 'string' ? `'${v}'` : v}`)
      .join(', ');
    const whereClause = Object.entries(where)
      .map(([k, v]) => `${k} = ${typeof v === 'string' ? `'${v}'` : v}`)
      .join(' AND ');
    return this.execute(`UPDATE ${table} SET ${setClause} WHERE ${whereClause}`);
  }

  /**
   * Delete rows
   */
  async delete(
    table: string,
    where: Record<string, unknown>
  ): Promise<ExecuteResult> {
    const whereClause = Object.entries(where)
      .map(([k, v]) => `${k} = ${typeof v === 'string' ? `'${v}'` : v}`)
      .join(' AND ');
    return this.execute(`DELETE FROM ${table} WHERE ${whereClause}`);
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
  async measureColdStart(
    newDbName: string,
    iterations = 5
  ): Promise<LatencyMeasurement> {
    const samples: number[] = [];

    for (let i = 0; i < iterations; i++) {
      // Use a unique DB name to ensure cold start
      const uniqueDbName = `${newDbName}-cold-${i}-${Date.now()}`;
      const url = `${this.config.baseUrl}/db/${uniqueDbName}/health`;

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

  /**
   * Measure throughput (requests per second)
   */
  async measureThroughput(
    operation: () => Promise<unknown>,
    durationMs = 5000,
    concurrency = 10
  ): Promise<{
    totalRequests: number;
    successfulRequests: number;
    failedRequests: number;
    requestsPerSecond: number;
    avgLatencyMs: number;
    errors: string[];
  }> {
    const startTime = Date.now();
    const endTime = startTime + durationMs;
    let totalRequests = 0;
    let successfulRequests = 0;
    let failedRequests = 0;
    const errors: string[] = [];
    const latencies: number[] = [];

    async function worker(): Promise<void> {
      while (Date.now() < endTime) {
        const reqStart = performance.now();
        try {
          await operation();
          successfulRequests++;
          latencies.push(performance.now() - reqStart);
        } catch (error) {
          failedRequests++;
          errors.push(error instanceof Error ? error.message : String(error));
        }
        totalRequests++;
      }
    }

    // Run concurrent workers
    const workers = Array.from({ length: concurrency }, () => worker());
    await Promise.all(workers);

    const actualDuration = Date.now() - startTime;

    return {
      totalRequests,
      successfulRequests,
      failedRequests,
      requestsPerSecond: (totalRequests / actualDuration) * 1000,
      avgLatencyMs:
        latencies.length > 0
          ? latencies.reduce((a, b) => a + b, 0) / latencies.length
          : 0,
      errors: [...new Set(errors)], // Deduplicate errors
    };
  }
}

// =============================================================================
// WebSocket Client
// =============================================================================

/**
 * WebSocket client for CDC streaming
 */
export class DoSQLWebSocketClient {
  private config: Required<E2EClientConfig>;
  private ws: WebSocket | null = null;
  private messageQueue: CDCEvent[] = [];
  private waitingResolvers: Array<(event: CDCEvent) => void> = [];
  private connected = false;

  constructor(config: E2EClientConfig) {
    this.config = {
      timeoutMs: 30_000,
      debug: false,
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
      const fullUrl = `${wsUrl}/db/${this.config.dbName}/ws`;

      if (this.config.debug) {
        console.log(`[E2E WS] Connecting to ${fullUrl}`);
      }

      this.ws = new WebSocket(fullUrl);

      const timeout = setTimeout(() => {
        reject(new Error(`WebSocket connection timeout after ${this.config.timeoutMs}ms`));
      }, this.config.timeoutMs);

      this.ws.onopen = () => {
        clearTimeout(timeout);
        this.connected = true;
        if (this.config.debug) {
          console.log('[E2E WS] Connected');
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
          console.log('[E2E WS] Disconnected');
        }
      };

      this.ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data) as CDCEvent;
          if (this.config.debug) {
            console.log('[E2E WS] Received:', data);
          }

          // If someone is waiting for an event, deliver it
          const resolver = this.waitingResolvers.shift();
          if (resolver) {
            resolver(data);
          } else {
            this.messageQueue.push(data);
          }
        } catch (error) {
          console.error('[E2E WS] Failed to parse message:', error);
        }
      };
    });
  }

  /**
   * Subscribe to CDC events
   */
  async subscribeCDC(options: {
    fromLSN?: bigint;
    tables?: string[];
    operations?: string[];
  } = {}): Promise<void> {
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
   * Wait for the next CDC event
   */
  async nextEvent(timeoutMs = 10_000): Promise<CDCEvent> {
    // Check queue first
    const queued = this.messageQueue.shift();
    if (queued) {
      return queued;
    }

    // Wait for next event
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this.waitingResolvers.indexOf(resolve);
        if (index >= 0) {
          this.waitingResolvers.splice(index, 1);
        }
        reject(new Error(`Timeout waiting for CDC event after ${timeoutMs}ms`));
      }, timeoutMs);

      this.waitingResolvers.push((event) => {
        clearTimeout(timeout);
        resolve(event);
      });
    });
  }

  /**
   * Collect events for a duration
   */
  async collectEvents(durationMs: number): Promise<CDCEvent[]> {
    const events: CDCEvent[] = [...this.messageQueue];
    this.messageQueue.length = 0;

    const endTime = Date.now() + durationMs;

    while (Date.now() < endTime) {
      try {
        const event = await this.nextEvent(Math.max(100, endTime - Date.now()));
        events.push(event);
      } catch {
        // Timeout, continue
      }
    }

    return events;
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
 * Create a new E2E client for a test database
 */
export function createTestClient(
  baseUrl: string,
  testName: string,
  options: { debug?: boolean } = {}
): DoSQLE2EClient {
  const dbName = `test-${testName}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
  return new DoSQLE2EClient({
    baseUrl,
    dbName,
    debug: options.debug ?? false,
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
 * Assert helper for E2E tests
 */
export function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(`E2E Assertion failed: ${message}`);
  }
}

/**
 * Assert that a value equals expected
 */
export function assertEqual<T>(actual: T, expected: T, message?: string): void {
  if (actual !== expected) {
    throw new Error(
      `E2E Assertion failed: ${message || 'Values not equal'}\n` +
      `  Expected: ${JSON.stringify(expected)}\n` +
      `  Actual: ${JSON.stringify(actual)}`
    );
  }
}

/**
 * Assert that an array has expected length
 */
export function assertLength(arr: unknown[], expected: number, message?: string): void {
  if (arr.length !== expected) {
    throw new Error(
      `E2E Assertion failed: ${message || 'Array length mismatch'}\n` +
      `  Expected length: ${expected}\n` +
      `  Actual length: ${arr.length}`
    );
  }
}

/**
 * Assert that an operation completes within a time limit
 */
export async function assertWithinTime<T>(
  operation: () => Promise<T>,
  maxMs: number,
  message?: string
): Promise<T> {
  const start = performance.now();
  const result = await operation();
  const elapsed = performance.now() - start;

  if (elapsed > maxMs) {
    throw new Error(
      `E2E Assertion failed: ${message || 'Operation too slow'}\n` +
      `  Max time: ${maxMs}ms\n` +
      `  Actual time: ${elapsed.toFixed(2)}ms`
    );
  }

  return result;
}

// =============================================================================
// Exports
// =============================================================================

export type { E2EClientConfig, QueryResult, ExecuteResult, HealthResponse, TablesResponse };
