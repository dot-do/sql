/**
 * Tail Worker CDC Streamer
 *
 * Transforms Cloudflare Tail Worker telemetry events into CDC format
 * and streams them to DoLake DO pool via WebSocket.
 *
 * Issue: do-d1isn.3 - Tail worker CDC streaming
 *
 * Architecture:
 * ```
 * Worker Telemetry → Tail Worker → ShardRouter → WebSocket → DoLake DO Pool → R2
 *                       ↓
 *                  Transform to CDC
 *                       ↓
 *                   Batch by shard
 * ```
 *
 * Features:
 * - TraceItem event capture from workers
 * - CDC format transformation with P3 durability
 * - Consistent hashing for shard routing
 * - WebSocket streaming with backpressure handling
 * - Event dropping under severe load (P3 events only)
 */

import { generateUUID, type CDCEvent, type CDCBatchMessage } from './types.js';

// =============================================================================
// TraceItem Type (Cloudflare Tail Worker Event Format)
// =============================================================================

/**
 * Cloudflare Tail Worker TraceItem format
 */
export interface TraceItem {
  /** Name of the worker script */
  scriptName: string;
  /** Script version (optional) */
  scriptVersion?: string;
  /** Dispatch namespace (optional) */
  dispatchNamespace?: string;
  /** Exceptions thrown during execution */
  exceptions: Array<{
    name: string;
    message: string;
    timestamp: number;
  }>;
  /** Console logs emitted during execution */
  logs: Array<{
    level: 'debug' | 'info' | 'log' | 'warn' | 'error';
    message: string[];
    timestamp: number;
  }>;
  /** Event that triggered the worker */
  event: {
    request?: {
      url: string;
      method: string;
      headers?: Record<string, string>;
      cf?: Record<string, unknown>;
    };
    response?: {
      status: number;
    };
    scheduledTime?: number;
    cron?: string;
  };
  /** Timestamp when event was received by the worker */
  eventTimestamp: number;
  /** Execution outcome */
  outcome: 'ok' | 'exception' | 'exceededCpu' | 'exceededMemory' | 'unknown' | 'canceled' | 'scriptNotFound';
  /** CPU time consumed in milliseconds */
  cpuTime?: number;
  /** Wall time in milliseconds */
  wallTime?: number;
}

/**
 * Batch of trace items from tail worker
 */
export interface TailWorkerBatch {
  traces: TraceItem[];
}

// =============================================================================
// CDC Format for Traces
// =============================================================================

/**
 * Durability tier for events
 */
export type DurabilityTier = 'P0' | 'P1' | 'P2' | 'P3';

/**
 * CDC event format for worker traces
 */
export interface TraceCDCEvent extends CDCEvent {
  /** Extended metadata for traces */
  metadata?: {
    /** Event type identifier */
    $type?: 'worker.trace';
    /** Durability tier for telemetry (P3 = best effort) */
    durabilityTier?: DurabilityTier;
  };
}

// =============================================================================
// Configuration
// =============================================================================

/**
 * Configuration for TailWorkerCDCStreamer
 */
export interface TailWorkerConfig {
  /** Default namespace for routing */
  defaultNamespace: string;
  /** Number of shards for trace data */
  shardCount: number;
  /** Maximum batch size before sending */
  maxBatchSize: number;
  /** Maximum time to hold events before sending (ms) */
  maxBatchDelayMs: number;
  /** Buffer capacity per shard (percentage 0-1) */
  backpressureThreshold: number;
  /** Maximum pending messages before dropping P3 events */
  maxPendingMessages: number;
  /** WebSocket URL pattern for DoLake connections */
  doLakeUrlPattern: string;
  /** Timeout for ack response (ms) */
  ackTimeoutMs: number;
  /** Maximum retry attempts */
  maxRetries: number;
  /** Initial retry delay (ms) */
  initialRetryDelayMs: number;
  /** Maximum retry delay (ms) */
  maxRetryDelayMs: number;
}

/**
 * Default tail worker configuration
 */
export const DEFAULT_TAIL_WORKER_CONFIG: TailWorkerConfig = {
  defaultNamespace: 'default',
  shardCount: 8,
  maxBatchSize: 100,
  maxBatchDelayMs: 5000,
  backpressureThreshold: 0.8,
  maxPendingMessages: 1000,
  doLakeUrlPattern: 'wss://dolake-{shard}.workers.dev',
  ackTimeoutMs: 30000,
  maxRetries: 3,
  initialRetryDelayMs: 1000,
  maxRetryDelayMs: 30000,
};

// =============================================================================
// Result Types
// =============================================================================

/**
 * Backpressure status for a shard
 */
export interface ShardBackpressureStatus {
  shardId: string;
  pendingMessages: number;
  bufferUtilization: number;
  isBackpressured: boolean;
  droppedEvents: number;
}

/**
 * CDC transformation result
 */
export interface TransformResult {
  event: TraceCDCEvent;
  shardId: string;
  partitionKey: string;
}

/**
 * Batch send result
 */
export interface BatchSendResult {
  shardId: string;
  success: boolean;
  eventsCount: number;
  bytesSize: number;
  durationMs: number;
  error?: string;
  wasDropped?: boolean;
  dropReason?: 'backpressure' | 'connection_failed' | 'timeout';
}

/**
 * Load metrics for auto-scaling
 */
export interface TailWorkerLoadMetrics {
  bufferUtilization: number;
  avgLatencyMs: number;
  requestsPerSecond: number;
  activeConnections: number;
  p99LatencyMs: number;
}

// =============================================================================
// Shard State
// =============================================================================

/**
 * Per-shard state management
 */
interface ShardState {
  shardId: string;
  pendingMessages: number;
  droppedEvents: number;
  sequenceNumber: number;
  isConnected: boolean;
  reconnectAttempts: number;
  buffer: TraceCDCEvent[];
  lastFlushTime: number;
  latencies: number[];
}

// =============================================================================
// TailWorkerCDCStreamer Implementation
// =============================================================================

/**
 * TailWorkerCDCStreamer - Transforms and streams worker telemetry to DoLake
 *
 * @example
 * ```typescript
 * const streamer = new TailWorkerCDCStreamer({
 *   shardCount: 8,
 *   maxBatchSize: 100,
 * });
 *
 * // In tail worker handler
 * export default {
 *   async tail(events: TraceItem[]) {
 *     await streamer.processBatch({ traces: events });
 *   }
 * };
 * ```
 */
export class TailWorkerCDCStreamer {
  private config: TailWorkerConfig;
  private shardStates: Map<string, ShardState> = new Map();
  private connections: Map<string, WebSocket> = new Map();
  private durabilityOverrides: Map<string, DurabilityTier> = new Map();
  private flushTimers: Map<string, ReturnType<typeof setTimeout>> = new Map();
  private totalDroppedEvents: number = 0;
  private requestCount: number = 0;
  private requestStartTime: number = Date.now();

  constructor(config: Partial<TailWorkerConfig> = {}) {
    this.config = {
      ...DEFAULT_TAIL_WORKER_CONFIG,
      ...config,
    };
    this.initializeShards();
  }

  /**
   * Initialize shard states
   */
  private initializeShards(): void {
    for (let i = 0; i < this.config.shardCount; i++) {
      const shardId = `shard-${i}`;
      this.shardStates.set(shardId, {
        shardId,
        pendingMessages: 0,
        droppedEvents: 0,
        sequenceNumber: 0,
        isConnected: false,
        reconnectAttempts: 0,
        buffer: [],
        lastFlushTime: Date.now(),
        latencies: [],
      });
    }
  }

  // ===========================================================================
  // Core Processing
  // ===========================================================================

  /**
   * Process a batch of trace items from the tail worker
   */
  async processBatch(batch: TailWorkerBatch): Promise<BatchSendResult[]> {
    this.requestCount++;
    const results: BatchSendResult[] = [];

    // Group events by shard
    const shardBatches = new Map<string, TraceCDCEvent[]>();

    for (const trace of batch.traces) {
      try {
        const { event, shardId } = this.transformToCDC(trace);

        // Check backpressure
        if (this.shouldDropEvent(shardId, event)) {
          const state = this.shardStates.get(shardId);
          if (state) {
            state.droppedEvents++;
            this.totalDroppedEvents++;
          }
          results.push({
            shardId,
            success: false,
            eventsCount: 1,
            bytesSize: 0,
            durationMs: 0,
            wasDropped: true,
            dropReason: 'backpressure',
          });
          continue;
        }

        // Add to shard batch
        const existing = shardBatches.get(shardId) ?? [];
        existing.push(event);
        shardBatches.set(shardId, existing);
      } catch (error) {
        // Handle malformed traces gracefully
        results.push({
          shardId: 'unknown',
          success: false,
          eventsCount: 1,
          bytesSize: 0,
          durationMs: 0,
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      }
    }

    // Send batches to each shard
    for (const [shardId, events] of shardBatches) {
      const shardResults = await this.sendToShard(shardId, events);
      results.push(...shardResults);
    }

    return results;
  }

  /**
   * Transform a TraceItem to CDC format
   */
  transformToCDC(trace: TraceItem): TransformResult {
    const eventId = generateUUID();
    const timestamp = trace.eventTimestamp ?? Date.now();

    // Determine durability tier
    const durabilityTier = this.durabilityOverrides.get(trace.scriptName) ?? 'P3';

    // Create CDC event
    const event: TraceCDCEvent = {
      sequence: 0, // Will be set when sending
      timestamp,
      operation: 'INSERT',
      table: 'worker_traces',
      rowId: eventId,
      after: {
        id: eventId,
        scriptName: trace.scriptName,
        scriptVersion: trace.scriptVersion,
        dispatchNamespace: trace.dispatchNamespace,
        outcome: trace.outcome,
        cpuTime: trace.cpuTime,
        wallTime: trace.wallTime,
        exceptions: trace.exceptions,
        logs: trace.logs,
        event: trace.event,
        eventTimestamp: timestamp,
        request: trace.event?.request,
        response: trace.event?.response,
      },
      metadata: {
        $type: 'worker.trace',
        durabilityTier,
      },
    };

    // Compute shard based on script name
    const shardId = this.getTargetShard(trace);

    // Compute partition key (date + script name for time-based partitioning)
    const date = new Date(timestamp);
    const dateStr = date.toISOString().split('T')[0]; // YYYY-MM-DD
    const partitionKey = `${dateStr}/${trace.scriptName}`;

    return { event, shardId, partitionKey };
  }

  /**
   * Get the target shard for a trace item using consistent hashing
   */
  getTargetShard(trace: TraceItem): string {
    // Use script name + dispatch namespace for routing
    const routingKey = trace.dispatchNamespace
      ? `${trace.dispatchNamespace}:${trace.scriptName}`
      : trace.scriptName;

    const hash = this.fnv1aHash(routingKey);
    const shardIndex = hash % this.config.shardCount;

    return `shard-${shardIndex}`;
  }

  /**
   * FNV-1a hash for consistent shard assignment
   */
  private fnv1aHash(str: string): number {
    const FNV_OFFSET_BASIS = 2166136261;
    const FNV_PRIME = 16777619;

    let hash = FNV_OFFSET_BASIS;

    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = (hash * FNV_PRIME) >>> 0;
    }

    return hash;
  }

  // ===========================================================================
  // Backpressure Handling
  // ===========================================================================

  /**
   * Check if an event should be dropped due to backpressure
   */
  private shouldDropEvent(shardId: string, event: TraceCDCEvent): boolean {
    const state = this.shardStates.get(shardId);
    if (!state) return false;

    const utilization = state.pendingMessages / this.config.maxPendingMessages;

    // Only drop P3 (telemetry) events
    const durabilityTier = event.metadata?.durabilityTier ?? 'P3';
    if (durabilityTier !== 'P3') return false;

    // Drop if above backpressure threshold
    return utilization >= this.config.backpressureThreshold;
  }

  /**
   * Get backpressure status for all shards
   */
  getBackpressureStatus(): Map<string, ShardBackpressureStatus> {
    const status = new Map<string, ShardBackpressureStatus>();

    for (const [shardId, state] of this.shardStates) {
      const utilization = state.pendingMessages / this.config.maxPendingMessages;
      status.set(shardId, {
        shardId,
        pendingMessages: state.pendingMessages,
        bufferUtilization: utilization,
        isBackpressured: utilization >= this.config.backpressureThreshold,
        droppedEvents: state.droppedEvents,
      });
    }

    return status;
  }

  /**
   * Check if a specific shard is experiencing backpressure
   */
  isBackpressured(shardId: string): boolean {
    const state = this.shardStates.get(shardId);
    if (!state) return false;

    const utilization = state.pendingMessages / this.config.maxPendingMessages;
    return utilization >= this.config.backpressureThreshold;
  }

  /**
   * Simulate pending messages for testing
   */
  simulatePendingMessages(shardId: string, count: number): void {
    const state = this.shardStates.get(shardId);
    if (state) {
      state.pendingMessages = count;
    }
  }

  // ===========================================================================
  // WebSocket Communication
  // ===========================================================================

  /**
   * Send events to a specific shard
   */
  private async sendToShard(shardId: string, events: TraceCDCEvent[]): Promise<BatchSendResult[]> {
    const results: BatchSendResult[] = [];
    const state = this.shardStates.get(shardId);
    if (!state) return results;

    // Split into batches if needed
    const batches: TraceCDCEvent[][] = [];
    for (let i = 0; i < events.length; i += this.config.maxBatchSize) {
      batches.push(events.slice(i, i + this.config.maxBatchSize));
    }

    for (const batch of batches) {
      const startTime = Date.now();
      state.sequenceNumber++;

      const message: CDCBatchMessage = {
        type: 'cdc_batch',
        timestamp: Date.now(),
        sourceDoId: `tail-worker-${this.config.defaultNamespace}`,
        events: batch.map((e, idx) => ({
          ...e,
          sequence: state.sequenceNumber * 1000 + idx,
        })),
        sequenceNumber: state.sequenceNumber,
        firstEventSequence: state.sequenceNumber * 1000,
        lastEventSequence: state.sequenceNumber * 1000 + batch.length - 1,
        sizeBytes: JSON.stringify(batch).length,
        isRetry: false,
        retryCount: 0,
      };

      try {
        const ws = this.connections.get(shardId);
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          // Connection not available
          const connected = await this.connectToShard(shardId);
          if (!connected) {
            results.push({
              shardId,
              success: false,
              eventsCount: batch.length,
              bytesSize: message.sizeBytes,
              durationMs: Date.now() - startTime,
              error: 'Connection failed',
              wasDropped: true,
              dropReason: 'connection_failed',
            });
            continue;
          }
        }

        // Send with retry
        const success = await this.sendWithRetry(shardId, message);
        const durationMs = Date.now() - startTime;

        // Track latency
        state.latencies.push(durationMs);
        if (state.latencies.length > 100) {
          state.latencies.shift();
        }

        results.push({
          shardId,
          success,
          eventsCount: batch.length,
          bytesSize: message.sizeBytes,
          durationMs,
          error: success ? undefined : 'Send failed after retries',
        });

        if (success) {
          state.pendingMessages += batch.length;
        }
      } catch (error) {
        results.push({
          shardId,
          success: false,
          eventsCount: batch.length,
          bytesSize: message.sizeBytes,
          durationMs: Date.now() - startTime,
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      }
    }

    return results;
  }

  /**
   * Send message with retry and exponential backoff
   */
  private async sendWithRetry(shardId: string, message: CDCBatchMessage): Promise<boolean> {
    const state = this.shardStates.get(shardId);
    let attempts = 0;

    while (attempts < this.config.maxRetries) {
      try {
        const ws = this.connections.get(shardId);
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          throw new Error('WebSocket not open');
        }

        ws.send(JSON.stringify(message));

        // TODO: Wait for ACK with timeout
        // For now, we assume success if send doesn't throw
        if (state) {
          state.reconnectAttempts = 0;
        }
        return true;
      } catch (error) {
        attempts++;
        if (attempts < this.config.maxRetries) {
          const delay = this.getReconnectDelay(shardId, attempts);
          await this.sleep(delay);

          // Try to reconnect
          await this.connectToShard(shardId);
        }
      }
    }

    return false;
  }

  /**
   * Connect to a DoLake shard
   */
  private async connectToShard(shardId: string): Promise<boolean> {
    const state = this.shardStates.get(shardId);
    if (!state) return false;

    try {
      const url = this.config.doLakeUrlPattern.replace('{shard}', shardId);
      const ws = new WebSocket(url);

      return new Promise((resolve) => {
        const timeout = setTimeout(() => {
          ws.close();
          resolve(false);
        }, this.config.ackTimeoutMs);

        ws.addEventListener('open', () => {
          clearTimeout(timeout);
          this.connections.set(shardId, ws);
          state.isConnected = true;
          state.reconnectAttempts = 0;
          resolve(true);
        });

        ws.addEventListener('error', () => {
          clearTimeout(timeout);
          state.reconnectAttempts++;
          resolve(false);
        });

        ws.addEventListener('close', () => {
          state.isConnected = false;
          this.connections.delete(shardId);
        });

        // Handle ACKs
        ws.addEventListener('message', (event) => {
          this.handleAck(shardId, event.data);
        });
      });
    } catch {
      state.reconnectAttempts++;
      return false;
    }
  }

  /**
   * Handle ACK message from DoLake
   */
  private handleAck(shardId: string, data: unknown): void {
    const state = this.shardStates.get(shardId);
    if (!state) return;

    try {
      const message = typeof data === 'string' ? JSON.parse(data) : data;
      if (message.type === 'ack') {
        // Decrease pending count based on acknowledged events
        const ackedCount = message.details?.eventsProcessed ?? 1;
        state.pendingMessages = Math.max(0, state.pendingMessages - ackedCount);
      }
    } catch {
      // Ignore parse errors
    }
  }

  /**
   * Calculate reconnect delay with exponential backoff
   */
  getReconnectDelay(shardId: string, attempt: number): number {
    const baseDelay = this.config.initialRetryDelayMs;
    const maxDelay = this.config.maxRetryDelayMs;

    // Exponential backoff: baseDelay * 2^attempt
    const delay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);

    // Add jitter (0-25% of delay)
    const jitter = delay * Math.random() * 0.25;

    return Math.floor(delay + jitter);
  }

  /**
   * Get reconnect attempts for a shard
   */
  getReconnectAttempts(shardId: string): number {
    return this.shardStates.get(shardId)?.reconnectAttempts ?? 0;
  }

  /**
   * Inject a connection for testing
   */
  injectConnection(shardId: string, ws: WebSocket): void {
    this.connections.set(shardId, ws);
    const state = this.shardStates.get(shardId);
    if (state) {
      state.isConnected = true;
    }
  }

  /**
   * Inject connection error for testing
   */
  injectConnectionError(shardId: string, error: Error): void {
    const state = this.shardStates.get(shardId);
    if (state) {
      state.isConnected = false;
      state.reconnectAttempts++;
    }
    this.connections.delete(shardId);
  }

  /**
   * Check if connected to a shard
   */
  isConnected(shardId: string): boolean {
    return this.shardStates.get(shardId)?.isConnected ?? false;
  }

  // ===========================================================================
  // Flush & Lifecycle
  // ===========================================================================

  /**
   * Buffer events without immediately sending
   */
  bufferEvents(batch: TailWorkerBatch): void {
    for (const trace of batch.traces) {
      const { event, shardId } = this.transformToCDC(trace);
      const state = this.shardStates.get(shardId);
      if (state) {
        state.buffer.push(event);
      }
    }

    // Set flush timers
    for (const [shardId, state] of this.shardStates) {
      if (state.buffer.length > 0 && !this.flushTimers.has(shardId)) {
        const timer = setTimeout(() => {
          this.flushShard(shardId);
        }, this.config.maxBatchDelayMs);
        this.flushTimers.set(shardId, timer);
      }
    }
  }

  /**
   * Flush all pending batches
   */
  async flush(): Promise<BatchSendResult[]> {
    const results: BatchSendResult[] = [];

    for (const [shardId, state] of this.shardStates) {
      if (state.buffer.length > 0) {
        const shardResults = await this.sendToShard(shardId, state.buffer);
        results.push(...shardResults);
        state.buffer = [];
        state.lastFlushTime = Date.now();
      }

      // Clear any pending flush timer
      const timer = this.flushTimers.get(shardId);
      if (timer) {
        clearTimeout(timer);
        this.flushTimers.delete(shardId);
      }
    }

    return results;
  }

  /**
   * Flush a specific shard
   */
  private async flushShard(shardId: string): Promise<void> {
    const state = this.shardStates.get(shardId);
    if (!state || state.buffer.length === 0) return;

    await this.sendToShard(shardId, state.buffer);
    state.buffer = [];
    state.lastFlushTime = Date.now();

    // Clear timer
    const timer = this.flushTimers.get(shardId);
    if (timer) {
      clearTimeout(timer);
      this.flushTimers.delete(shardId);
    }
  }

  /**
   * Close all connections gracefully
   */
  async close(): Promise<void> {
    // Flush all pending events
    await this.flush();

    // Clear all timers
    for (const timer of this.flushTimers.values()) {
      clearTimeout(timer);
    }
    this.flushTimers.clear();

    // Close all connections
    for (const [shardId, ws] of this.connections) {
      try {
        ws.close(1000, 'Graceful shutdown');
      } catch {
        // Ignore close errors
      }
      const state = this.shardStates.get(shardId);
      if (state) {
        state.isConnected = false;
      }
    }
    this.connections.clear();
  }

  // ===========================================================================
  // Statistics & Metrics
  // ===========================================================================

  /**
   * Get total dropped events count
   */
  getDroppedEventsCount(): number {
    return this.totalDroppedEvents;
  }

  /**
   * Get number of events currently buffered
   */
  getBufferedEventsCount(): number {
    let count = 0;
    for (const state of this.shardStates.values()) {
      count += state.buffer.length;
    }
    return count;
  }

  /**
   * Get number of pending events across all shards
   */
  getPendingEventsCount(): number {
    let count = 0;
    for (const state of this.shardStates.values()) {
      count += state.pendingMessages;
    }
    return count;
  }

  /**
   * Get load metrics for auto-scaling
   */
  getLoadMetrics(): TailWorkerLoadMetrics {
    let totalBuffer = 0;
    let totalLatency = 0;
    let latencyCount = 0;
    let maxLatency = 0;

    for (const state of this.shardStates.values()) {
      totalBuffer += state.pendingMessages;
      for (const latency of state.latencies) {
        totalLatency += latency;
        latencyCount++;
        maxLatency = Math.max(maxLatency, latency);
      }
    }

    const avgLatency = latencyCount > 0 ? totalLatency / latencyCount : 0;
    const bufferUtilization = totalBuffer / (this.config.maxPendingMessages * this.config.shardCount);

    const elapsed = (Date.now() - this.requestStartTime) / 1000;
    const rps = elapsed > 0 ? this.requestCount / elapsed : 0;

    return {
      bufferUtilization,
      avgLatencyMs: avgLatency,
      p99LatencyMs: maxLatency, // Simplified p99 approximation
      requestsPerSecond: rps,
      activeConnections: this.connections.size,
    };
  }

  // ===========================================================================
  // Scaling Support
  // ===========================================================================

  /**
   * Update shard count (for scaling)
   */
  updateShardCount(newCount: number): void {
    const oldCount = this.config.shardCount;
    this.config.shardCount = newCount;

    // Add new shards if scaling up
    for (let i = oldCount; i < newCount; i++) {
      const shardId = `shard-${i}`;
      this.shardStates.set(shardId, {
        shardId,
        pendingMessages: 0,
        droppedEvents: 0,
        sequenceNumber: 0,
        isConnected: false,
        reconnectAttempts: 0,
        buffer: [],
        lastFlushTime: Date.now(),
        latencies: [],
      });
    }

    // Note: When scaling down, existing shards remain until drained
  }

  /**
   * Scale to a specific number of shards
   */
  async scaleTo(targetCount: number): Promise<void> {
    const currentCount = this.config.shardCount;

    if (targetCount < currentCount) {
      // Drain and remove excess shards
      for (let i = targetCount; i < currentCount; i++) {
        const shardId = `shard-${i}`;
        await this.flushShard(shardId);

        // Close connection
        const ws = this.connections.get(shardId);
        if (ws) {
          ws.close(1000, 'Scaling down');
          this.connections.delete(shardId);
        }

        this.shardStates.delete(shardId);
      }
    }

    this.updateShardCount(targetCount);
  }

  /**
   * Get active shard count
   */
  getActiveShardCount(): number {
    return this.shardStates.size;
  }

  /**
   * Set durability override for a script
   */
  setDurabilityOverride(scriptName: string, tier: DurabilityTier): void {
    this.durabilityOverrides.set(scriptName, tier);
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a TailWorkerCDCStreamer with default or custom configuration
 */
export function createTailWorkerCDCStreamer(
  config?: Partial<TailWorkerConfig>
): TailWorkerCDCStreamer {
  return new TailWorkerCDCStreamer(config);
}
