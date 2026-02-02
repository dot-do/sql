/**
 * CDC Consumer Connection Pool
 *
 * Provides connection pooling for CDC streaming consumers with:
 * - Configurable pool size and consumer limits
 * - Health monitoring for connected consumers
 * - Backpressure signaling when consumers fall behind
 * - Graceful connection lifecycle management
 *
 * @packageDocumentation
 */

import type {
  BackpressureSignal,
  CDCFilter,
  ChangeEvent,
  TransactionEvent,
  CDCEvent,
} from './types.js';

// =============================================================================
// Pool Configuration Types
// =============================================================================

/**
 * Configuration for a CDC consumer connection pool
 */
export interface CDCPoolConfig {
  /** Maximum number of concurrent consumers (default: 100) */
  maxConsumers: number;
  /** Maximum waiting consumers when pool is full (default: 50) */
  maxWaitingConsumers: number;
  /** Consumer idle timeout in ms before disconnect (default: 30000) */
  consumerIdleTimeout: number;
  /** Health check interval in ms (default: 10000) */
  healthCheckInterval: number;
  /** Buffer size per consumer before backpressure (default: 1000) */
  consumerBufferSize: number;
  /** Buffer utilization threshold for backpressure signal (0-1) (default: 0.8) */
  backpressureThreshold: number;
  /** Strategy when pool is exhausted: 'queue' | 'reject' (default: 'queue') */
  backpressureStrategy: 'queue' | 'reject';
  /** Time to wait for slot in queue (ms) (default: 30000) */
  waitTimeout: number;
  /** Keepalive interval for consumers in ms (default: 15000) */
  keepAliveInterval: number;
}

/**
 * Default pool configuration
 */
export const DEFAULT_CDC_POOL_CONFIG: CDCPoolConfig = {
  maxConsumers: 100,
  maxWaitingConsumers: 50,
  consumerIdleTimeout: 30000,
  healthCheckInterval: 10000,
  consumerBufferSize: 1000,
  backpressureThreshold: 0.8,
  backpressureStrategy: 'queue',
  waitTimeout: 30000,
  keepAliveInterval: 15000,
};

// =============================================================================
// Consumer Types
// =============================================================================

/**
 * Consumer state tracking
 */
export type ConsumerState = 'connecting' | 'active' | 'paused' | 'disconnecting' | 'disconnected';

/**
 * A CDC consumer in the pool
 */
export interface CDCConsumer {
  /** Unique consumer identifier */
  id: string;
  /** Consumer name (optional, for debugging) */
  name?: string | undefined;
  /** Current state */
  state: ConsumerState;
  /** When the consumer connected */
  connectedAt: number;
  /** Last activity timestamp */
  lastActivityAt: number;
  /** Last acknowledged LSN */
  acknowledgedLSN: bigint;
  /** Number of events pending in buffer */
  pendingEvents: number;
  /** Current buffer utilization (0-1) */
  bufferUtilization: number;
  /** Whether backpressure is active for this consumer */
  backpressureActive: boolean;
  /** Filter applied to this consumer */
  filter?: CDCFilter | undefined;
  /** Custom metadata */
  metadata?: Record<string, string> | undefined;
}

/**
 * Internal consumer with buffer management
 * @internal
 */
interface InternalConsumer extends CDCConsumer {
  /** Event buffer */
  buffer: CDCEvent[];
  /** Maximum buffer size */
  maxBufferSize: number;
  /** Callback when consumer is ready for more events */
  onReady?: (() => void) | undefined;
  /** Callback when consumer receives events */
  onEvents?: ((events: CDCEvent[]) => Promise<void>) | undefined;
  /** Callback when backpressure changes */
  onBackpressure?: ((signal: BackpressureSignal) => void) | undefined;
}

/**
 * Waiter in the consumer queue
 * @internal
 */
interface ConsumerWaiter {
  resolve: (consumer: CDCConsumer) => void;
  reject: (error: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
  createdAt: number;
  options?: CDCConsumerOptions;
}

// =============================================================================
// Pool Statistics & Events
// =============================================================================

/**
 * Statistics for the CDC consumer pool
 */
export interface CDCPoolStats {
  /** Total consumers in pool */
  totalConsumers: number;
  /** Active consumers (processing events) */
  activeConsumers: number;
  /** Paused consumers (backpressure applied) */
  pausedConsumers: number;
  /** Consumers waiting for a slot */
  waitingConsumers: number;
  /** Maximum pool size */
  maxConsumers: number;
  /** Total consumers registered since pool created */
  totalConsumersRegistered: number;
  /** Total consumers that have disconnected */
  totalConsumersDisconnected: number;
  /** Total events delivered */
  totalEventsDelivered: number;
  /** Current backpressure rate (0-1, percentage of consumers with backpressure) */
  backpressureRate: number;
  /** Average buffer utilization across consumers */
  averageBufferUtilization: number;
}

/**
 * Health status of the CDC consumer pool
 */
export interface CDCPoolHealth {
  /** Overall pool health */
  healthy: boolean;
  /** Number of healthy consumers */
  healthyConsumers: number;
  /** Number of consumers with issues */
  unhealthyConsumers: number;
  /** Consumers experiencing backpressure */
  backpressuredConsumers: number;
  /** Last health check timestamp */
  lastHealthCheck: Date | null;
  /** Average event processing latency (ms) */
  averageLatency: number;
}

/**
 * Events emitted by the CDC consumer pool
 */
export interface CDCPoolEventMap {
  'consumer:registered': { consumerId: string; timestamp: Date };
  'consumer:disconnected': { consumerId: string; reason: string; timestamp: Date };
  'consumer:backpressure': { consumerId: string; signal: BackpressureSignal; timestamp: Date };
  'pool:health-check': CDCPoolHealth;
  'pool:backpressure': { backpressuredCount: number; totalConsumers: number; timestamp: Date };
}

/** Generic pool event listener type */
type CDCPoolEventListener = (event: CDCPoolEventMap[keyof CDCPoolEventMap]) => void;

// =============================================================================
// Consumer Options
// =============================================================================

/**
 * Options for registering a CDC consumer
 */
export interface CDCConsumerOptions {
  /** Optional consumer name for debugging */
  name?: string;
  /** Starting LSN for this consumer */
  fromLSN?: bigint;
  /** Filter for this consumer */
  filter?: CDCFilter;
  /** Custom buffer size (overrides pool default) */
  bufferSize?: number;
  /** Custom metadata */
  metadata?: Record<string, string>;
  /** Callback when events are available */
  onEvents?: (events: CDCEvent[]) => Promise<void>;
  /** Callback when backpressure status changes */
  onBackpressure?: (signal: BackpressureSignal) => void;
}

// =============================================================================
// CDC Consumer Pool Implementation
// =============================================================================

/**
 * Pool for managing CDC streaming consumers
 *
 * Handles:
 * - Consumer registration and lifecycle
 * - Buffer management per consumer
 * - Backpressure signaling
 * - Health monitoring
 *
 * @example
 * ```typescript
 * const pool = new CDCConsumerPool({
 *   maxConsumers: 50,
 *   backpressureThreshold: 0.8,
 * });
 *
 * // Register a consumer
 * const consumer = await pool.register({
 *   name: 'analytics-consumer',
 *   fromLSN: 0n,
 *   onEvents: async (events) => {
 *     // Process events
 *     for (const event of events) {
 *       console.log('Event:', event);
 *     }
 *   },
 *   onBackpressure: (signal) => {
 *     if (signal.type === 'pause') {
 *       console.log('Slowing down...');
 *     }
 *   },
 * });
 *
 * // Push events to all consumers
 * await pool.broadcast([event1, event2]);
 *
 * // Acknowledge processing
 * pool.acknowledge(consumer.id, lastLSN);
 *
 * // Disconnect when done
 * pool.disconnect(consumer.id);
 * ```
 */
export class CDCConsumerPool {
  private readonly config: CDCPoolConfig;

  // Consumer state
  private consumers: Map<string, InternalConsumer> = new Map();
  private waiters: ConsumerWaiter[] = [];
  private consumerCounter = 0;

  // Statistics
  private stats = {
    totalConsumersRegistered: 0,
    totalConsumersDisconnected: 0,
    totalEventsDelivered: 0,
  };

  // Timers
  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private keepAliveTimer: ReturnType<typeof setInterval> | null = null;
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;

  // Event listeners
  private eventListeners: Map<keyof CDCPoolEventMap, Set<CDCPoolEventListener>> = new Map();

  // State
  private closed = false;
  private lastHealthCheck: Date | null = null;

  /**
   * Creates a new CDC consumer pool
   *
   * @param config - Pool configuration options
   */
  constructor(config: Partial<CDCPoolConfig> = {}) {
    this.config = {
      ...DEFAULT_CDC_POOL_CONFIG,
      ...config,
    };

    // Initialize event listener maps
    const events: (keyof CDCPoolEventMap)[] = [
      'consumer:registered',
      'consumer:disconnected',
      'consumer:backpressure',
      'pool:health-check',
      'pool:backpressure',
    ];
    events.forEach(event => this.eventListeners.set(event, new Set()));

    // Start background tasks
    this.startBackgroundTasks();
  }

  // ===========================================================================
  // Public API - Consumer Management
  // ===========================================================================

  /**
   * Registers a new CDC consumer
   *
   * If the pool is full, behavior depends on backpressureStrategy:
   * - 'queue': Waits for a slot (up to waitTimeout)
   * - 'reject': Immediately rejects
   *
   * @param options - Consumer configuration
   * @returns The registered consumer
   * @throws {Error} When pool is closed, exhausted (reject strategy), or timeout
   */
  async register(options: CDCConsumerOptions = {}): Promise<CDCConsumer> {
    if (this.closed) {
      throw new Error('Pool is closed');
    }

    // Check if we can register immediately
    if (this.consumers.size < this.config.maxConsumers) {
      return this.createConsumer(options);
    }

    // Handle backpressure
    return this.handleRegistrationBackpressure(options);
  }

  /**
   * Disconnects a consumer from the pool
   *
   * @param consumerId - ID of the consumer to disconnect
   * @param reason - Reason for disconnection (for logging)
   */
  disconnect(consumerId: string, reason = 'Client requested'): void {
    const consumer = this.consumers.get(consumerId);
    if (!consumer) return;

    consumer.state = 'disconnected';
    this.consumers.delete(consumerId);
    this.stats.totalConsumersDisconnected++;

    this.emit('consumer:disconnected', {
      consumerId,
      reason,
      timestamp: new Date(),
    });

    // Check if any waiters can be served
    this.serveWaiters();
  }

  /**
   * Gets a consumer by ID
   *
   * @param consumerId - Consumer ID to look up
   * @returns The consumer or undefined if not found
   */
  getConsumer(consumerId: string): CDCConsumer | undefined {
    const internal = this.consumers.get(consumerId);
    if (!internal) return undefined;
    return this.toPublicConsumer(internal);
  }

  /**
   * Lists all consumers in the pool
   *
   * @returns Array of all consumers
   */
  listConsumers(): CDCConsumer[] {
    return Array.from(this.consumers.values()).map(c => this.toPublicConsumer(c));
  }

  // ===========================================================================
  // Public API - Event Distribution
  // ===========================================================================

  /**
   * Broadcasts events to all consumers
   *
   * Events are filtered per consumer and buffered if consumers are slow.
   * Returns backpressure information if any consumers are experiencing pressure.
   *
   * @param events - Events to broadcast
   * @returns Map of consumer ID to delivery result
   */
  async broadcast(events: CDCEvent[]): Promise<Map<string, { delivered: number; buffered: number; backpressure: boolean }>> {
    const results = new Map<string, { delivered: number; buffered: number; backpressure: boolean }>();

    for (const [consumerId, consumer] of this.consumers) {
      if (consumer.state === 'disconnected') continue;

      // Filter events for this consumer
      const filteredEvents = this.filterEventsForConsumer(events, consumer);
      if (filteredEvents.length === 0) {
        results.set(consumerId, { delivered: 0, buffered: 0, backpressure: consumer.backpressureActive });
        continue;
      }

      // Try to deliver or buffer
      const result = await this.deliverToConsumer(consumer, filteredEvents);
      results.set(consumerId, result);
    }

    // Check overall backpressure
    const backpressuredCount = Array.from(this.consumers.values())
      .filter(c => c.backpressureActive).length;

    if (backpressuredCount > 0) {
      this.emit('pool:backpressure', {
        backpressuredCount,
        totalConsumers: this.consumers.size,
        timestamp: new Date(),
      });
    }

    return results;
  }

  /**
   * Sends events to a specific consumer
   *
   * @param consumerId - Target consumer ID
   * @param events - Events to send
   * @returns Delivery result
   */
  async send(consumerId: string, events: CDCEvent[]): Promise<{ delivered: number; buffered: number; backpressure: boolean }> {
    const consumer = this.consumers.get(consumerId);
    if (!consumer) {
      throw new Error(`Consumer ${consumerId} not found`);
    }

    return this.deliverToConsumer(consumer, events);
  }

  /**
   * Acknowledges event processing up to a given LSN
   *
   * This allows the pool to track consumer progress and manage backpressure.
   *
   * @param consumerId - Consumer acknowledging
   * @param lsn - LSN up to which events are acknowledged
   */
  acknowledge(consumerId: string, lsn: bigint): void {
    const consumer = this.consumers.get(consumerId);
    if (!consumer) return;

    consumer.acknowledgedLSN = lsn;
    consumer.lastActivityAt = Date.now();

    // Clear acknowledged events from buffer
    consumer.buffer = consumer.buffer.filter(e => e.lsn > lsn);
    consumer.pendingEvents = consumer.buffer.length;

    // Update buffer utilization
    this.updateBufferUtilization(consumer);

    // Check if backpressure can be released
    if (consumer.backpressureActive && consumer.bufferUtilization < this.config.backpressureThreshold * 0.5) {
      this.releaseBackpressure(consumer);
    }
  }

  /**
   * Signals that a consumer is ready to receive more events
   *
   * @param consumerId - Consumer ID signaling readiness
   */
  signalReady(consumerId: string): void {
    const consumer = this.consumers.get(consumerId);
    if (!consumer) return;

    consumer.lastActivityAt = Date.now();

    // Try to flush buffered events
    if (consumer.buffer.length > 0 && consumer.onEvents) {
      this.flushConsumerBuffer(consumer);
    }
  }

  // ===========================================================================
  // Public API - Statistics & Health
  // ===========================================================================

  /**
   * Gets current pool statistics
   */
  getStats(): CDCPoolStats {
    const consumers = Array.from(this.consumers.values());
    const activeConsumers = consumers.filter(c => c.state === 'active').length;
    const pausedConsumers = consumers.filter(c => c.state === 'paused' || c.backpressureActive).length;
    const backpressuredCount = consumers.filter(c => c.backpressureActive).length;

    const totalBufferUtilization = consumers.reduce((sum, c) => sum + c.bufferUtilization, 0);
    const averageBufferUtilization = consumers.length > 0
      ? totalBufferUtilization / consumers.length
      : 0;

    return {
      totalConsumers: consumers.length,
      activeConsumers,
      pausedConsumers,
      waitingConsumers: this.waiters.length,
      maxConsumers: this.config.maxConsumers,
      totalConsumersRegistered: this.stats.totalConsumersRegistered,
      totalConsumersDisconnected: this.stats.totalConsumersDisconnected,
      totalEventsDelivered: this.stats.totalEventsDelivered,
      backpressureRate: consumers.length > 0 ? backpressuredCount / consumers.length : 0,
      averageBufferUtilization,
    };
  }

  /**
   * Gets pool health status
   */
  getHealth(): CDCPoolHealth {
    const consumers = Array.from(this.consumers.values());
    const healthyConsumers = consumers.filter(c =>
      c.state === 'active' && !c.backpressureActive
    ).length;
    const unhealthyConsumers = consumers.filter(c =>
      c.state !== 'active' && c.state !== 'paused'
    ).length;
    const backpressuredConsumers = consumers.filter(c => c.backpressureActive).length;

    return {
      healthy: unhealthyConsumers === 0 && consumers.length > 0,
      healthyConsumers,
      unhealthyConsumers,
      backpressuredConsumers,
      lastHealthCheck: this.lastHealthCheck,
      averageLatency: 0, // Would need latency tracking per consumer
    };
  }

  /**
   * Checks if the pool is closed
   */
  isClosed(): boolean {
    return this.closed;
  }

  // ===========================================================================
  // Public API - Event Handling
  // ===========================================================================

  /**
   * Registers an event listener
   */
  on<K extends keyof CDCPoolEventMap>(
    event: K,
    listener: (data: CDCPoolEventMap[K]) => void
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.add(listener as CDCPoolEventListener);
    }
    return this;
  }

  /**
   * Removes an event listener
   */
  off<K extends keyof CDCPoolEventMap>(
    event: K,
    listener: (data: CDCPoolEventMap[K]) => void
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.delete(listener as CDCPoolEventListener);
    }
    return this;
  }

  // ===========================================================================
  // Public API - Lifecycle
  // ===========================================================================

  /**
   * Closes the pool and disconnects all consumers
   */
  async close(): Promise<void> {
    this.closed = true;

    // Stop background tasks
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }
    if (this.idleCheckTimer) {
      clearInterval(this.idleCheckTimer);
      this.idleCheckTimer = null;
    }

    // Reject all waiters
    for (const waiter of this.waiters) {
      clearTimeout(waiter.timeoutId);
      waiter.reject(new Error('Pool closed'));
    }
    this.waiters = [];

    // Disconnect all consumers
    for (const [consumerId] of this.consumers) {
      this.disconnect(consumerId, 'Pool closing');
    }
  }

  // ===========================================================================
  // Private Methods - Consumer Management
  // ===========================================================================

  /**
   * Creates a new consumer
   * @internal
   */
  private createConsumer(options: CDCConsumerOptions): CDCConsumer {
    const id = `cdc-consumer-${++this.consumerCounter}-${Date.now().toString(36)}`;
    const now = Date.now();

    const consumer: InternalConsumer = {
      id,
      name: options.name,
      state: 'active',
      connectedAt: now,
      lastActivityAt: now,
      acknowledgedLSN: options.fromLSN ?? 0n,
      pendingEvents: 0,
      bufferUtilization: 0,
      backpressureActive: false,
      filter: options.filter,
      metadata: options.metadata,
      buffer: [],
      maxBufferSize: options.bufferSize ?? this.config.consumerBufferSize,
      onEvents: options.onEvents,
      onBackpressure: options.onBackpressure,
    };

    this.consumers.set(id, consumer);
    this.stats.totalConsumersRegistered++;

    this.emit('consumer:registered', {
      consumerId: id,
      timestamp: new Date(),
    });

    return this.toPublicConsumer(consumer);
  }

  /**
   * Handles registration when pool is full
   * @internal
   */
  private async handleRegistrationBackpressure(options: CDCConsumerOptions): Promise<CDCConsumer> {
    // With reject strategy, fail immediately when pool is full
    if (this.config.backpressureStrategy === 'reject') {
      throw new Error('Pool exhausted');
    }

    // Check waiter queue limits
    if (this.waiters.length >= this.config.maxWaitingConsumers) {
      throw new Error('Pool backpressure: too many waiting consumers');
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const index = this.waiters.findIndex(w => w.timeoutId === timeoutId);
        if (index !== -1) {
          this.waiters.splice(index, 1);
        }
        reject(new Error('Pool wait timeout'));
      }, this.config.waitTimeout);

      const waiter: ConsumerWaiter = {
        resolve,
        reject,
        timeoutId,
        createdAt: Date.now(),
        options,
      };

      this.waiters.push(waiter);
    });
  }

  /**
   * Serves waiting consumers when slots become available
   * @internal
   */
  private serveWaiters(): void {
    while (this.waiters.length > 0 && this.consumers.size < this.config.maxConsumers) {
      const waiter = this.waiters.shift()!;
      clearTimeout(waiter.timeoutId);

      try {
        const consumer = this.createConsumer(waiter.options ?? {});
        waiter.resolve(consumer);
      } catch (error) {
        waiter.reject(error instanceof Error ? error : new Error(String(error)));
      }
    }
  }

  /**
   * Converts internal consumer to public interface
   * @internal
   */
  private toPublicConsumer(internal: InternalConsumer): CDCConsumer {
    return {
      id: internal.id,
      name: internal.name,
      state: internal.state,
      connectedAt: internal.connectedAt,
      lastActivityAt: internal.lastActivityAt,
      acknowledgedLSN: internal.acknowledgedLSN,
      pendingEvents: internal.pendingEvents,
      bufferUtilization: internal.bufferUtilization,
      backpressureActive: internal.backpressureActive,
      filter: internal.filter,
      metadata: internal.metadata,
    };
  }

  // ===========================================================================
  // Private Methods - Event Delivery
  // ===========================================================================

  /**
   * Filters events for a specific consumer based on their filter
   * @internal
   */
  private filterEventsForConsumer(events: CDCEvent[], consumer: InternalConsumer): CDCEvent[] {
    if (!consumer.filter) return events;

    return events.filter(event => {
      // Only filter change events, not transaction events
      if (!('table' in event)) return true;

      const changeEvent = event as ChangeEvent;

      // Table filter
      if (consumer.filter!.tables && consumer.filter!.tables.length > 0) {
        if (!consumer.filter!.tables.includes(changeEvent.table)) {
          return false;
        }
      }

      return true;
    });
  }

  /**
   * Delivers events to a consumer, managing buffering and backpressure
   * @internal
   */
  private async deliverToConsumer(
    consumer: InternalConsumer,
    events: CDCEvent[]
  ): Promise<{ delivered: number; buffered: number; backpressure: boolean }> {
    let delivered = 0;
    let buffered = 0;

    // Check if consumer can receive events directly
    if (consumer.onEvents && !consumer.backpressureActive && consumer.buffer.length === 0) {
      try {
        await consumer.onEvents(events);
        delivered = events.length;
        this.stats.totalEventsDelivered += delivered;
        consumer.lastActivityAt = Date.now();
      } catch {
        // If delivery fails, buffer the events
        this.bufferEvents(consumer, events);
        buffered = events.length;
      }
    } else {
      // Buffer events
      this.bufferEvents(consumer, events);
      buffered = events.length;
    }

    // Update buffer utilization
    this.updateBufferUtilization(consumer);

    // Check if backpressure should be applied
    if (!consumer.backpressureActive && consumer.bufferUtilization >= this.config.backpressureThreshold) {
      this.applyBackpressure(consumer);
    }

    return {
      delivered,
      buffered,
      backpressure: consumer.backpressureActive,
    };
  }

  /**
   * Buffers events for a consumer
   * @internal
   */
  private bufferEvents(consumer: InternalConsumer, events: CDCEvent[]): void {
    // Add to buffer, respecting max size
    const spaceAvailable = consumer.maxBufferSize - consumer.buffer.length;
    const eventsToBuffer = events.slice(0, spaceAvailable);

    consumer.buffer.push(...eventsToBuffer);
    consumer.pendingEvents = consumer.buffer.length;
  }

  /**
   * Updates buffer utilization for a consumer
   * @internal
   */
  private updateBufferUtilization(consumer: InternalConsumer): void {
    consumer.bufferUtilization = consumer.buffer.length / consumer.maxBufferSize;
  }

  /**
   * Flushes buffered events to a consumer
   * @internal
   */
  private async flushConsumerBuffer(consumer: InternalConsumer): Promise<void> {
    if (!consumer.onEvents || consumer.buffer.length === 0) return;

    const events = [...consumer.buffer];
    consumer.buffer = [];
    consumer.pendingEvents = 0;

    try {
      await consumer.onEvents(events);
      this.stats.totalEventsDelivered += events.length;
      consumer.lastActivityAt = Date.now();
    } catch {
      // Re-buffer if delivery fails
      consumer.buffer = events.concat(consumer.buffer);
      consumer.pendingEvents = consumer.buffer.length;
    }

    this.updateBufferUtilization(consumer);
  }

  // ===========================================================================
  // Private Methods - Backpressure Management
  // ===========================================================================

  /**
   * Applies backpressure to a consumer
   * @internal
   */
  private applyBackpressure(consumer: InternalConsumer): void {
    if (consumer.backpressureActive) return;

    consumer.backpressureActive = true;
    consumer.state = 'paused';

    const signal: BackpressureSignal = {
      type: 'pause',
      bufferUtilization: consumer.bufferUtilization,
      suggestedDelayMs: Math.min(1000, consumer.buffer.length * 10),
      reason: `Buffer ${Math.round(consumer.bufferUtilization * 100)}% full`,
    };

    if (consumer.onBackpressure) {
      consumer.onBackpressure(signal);
    }

    this.emit('consumer:backpressure', {
      consumerId: consumer.id,
      signal,
      timestamp: new Date(),
    });
  }

  /**
   * Releases backpressure for a consumer
   * @internal
   */
  private releaseBackpressure(consumer: InternalConsumer): void {
    if (!consumer.backpressureActive) return;

    consumer.backpressureActive = false;
    consumer.state = 'active';

    const signal: BackpressureSignal = {
      type: 'resume',
      bufferUtilization: consumer.bufferUtilization,
      reason: 'Buffer drained',
    };

    if (consumer.onBackpressure) {
      consumer.onBackpressure(signal);
    }

    this.emit('consumer:backpressure', {
      consumerId: consumer.id,
      signal,
      timestamp: new Date(),
    });

    // Try to flush remaining buffer
    this.flushConsumerBuffer(consumer);
  }

  // ===========================================================================
  // Private Methods - Background Tasks
  // ===========================================================================

  /**
   * Starts background maintenance tasks
   * @internal
   */
  private startBackgroundTasks(): void {
    // Health check timer
    if (this.config.healthCheckInterval > 0) {
      this.healthCheckTimer = setInterval(() => {
        this.runHealthCheck();
      }, this.config.healthCheckInterval);
    }

    // Idle check timer
    if (this.config.consumerIdleTimeout > 0) {
      this.idleCheckTimer = setInterval(() => {
        this.cleanupIdleConsumers();
      }, Math.min(this.config.consumerIdleTimeout / 2, 10000));
    }

    // Keepalive timer (try to flush buffers)
    if (this.config.keepAliveInterval > 0) {
      this.keepAliveTimer = setInterval(() => {
        this.keepAliveConsumers();
      }, this.config.keepAliveInterval);
    }
  }

  /**
   * Runs health check on all consumers
   * @internal
   */
  private runHealthCheck(): void {
    this.lastHealthCheck = new Date();

    for (const consumer of this.consumers.values()) {
      // Check for stale consumers
      const idleTime = Date.now() - consumer.lastActivityAt;
      if (idleTime > this.config.consumerIdleTimeout * 2) {
        this.disconnect(consumer.id, 'Health check: consumer unresponsive');
        continue;
      }
    }

    this.emit('pool:health-check', this.getHealth());
  }

  /**
   * Cleans up idle consumers
   * @internal
   */
  private cleanupIdleConsumers(): void {
    const now = Date.now();

    for (const consumer of this.consumers.values()) {
      if (consumer.state === 'disconnected') continue;

      const idleTime = now - consumer.lastActivityAt;
      if (idleTime > this.config.consumerIdleTimeout) {
        this.disconnect(consumer.id, 'Idle timeout');
      }
    }
  }

  /**
   * Keeps consumers alive and tries to flush buffers
   * @internal
   */
  private keepAliveConsumers(): void {
    for (const consumer of this.consumers.values()) {
      if (consumer.state === 'disconnected') continue;

      // Try to flush buffered events
      if (consumer.buffer.length > 0 && consumer.onEvents) {
        this.flushConsumerBuffer(consumer);
      }
    }
  }

  // ===========================================================================
  // Private Methods - Event Emission
  // ===========================================================================

  /**
   * Emits an event to all listeners
   * @internal
   */
  private emit<K extends keyof CDCPoolEventMap>(event: K, data: CDCPoolEventMap[K]): void {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(data);
        } catch (error) {
          console.error(`Error in ${event} listener:`, error);
        }
      }
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Creates a new CDC consumer pool with the given configuration
 *
 * @param config - Pool configuration
 * @returns A new CDCConsumerPool instance
 */
export function createCDCConsumerPool(config: Partial<CDCPoolConfig> = {}): CDCConsumerPool {
  return new CDCConsumerPool(config);
}
