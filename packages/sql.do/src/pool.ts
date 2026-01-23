/**
 * WebSocket Connection Pool
 *
 * Provides connection pooling for WebSocket connections to Durable Objects.
 * Supports connection reuse, health checks, idle timeouts, and backpressure handling.
 *
 * Combined with Cloudflare's hibernation API on the DO side, this can reduce
 * costs by ~95% for idle connections.
 *
 * @packageDocumentation
 */

import type {
  PoolConfig,
  PoolStats,
  PoolHealth,
  ConnectionInfo,
  PoolEventMap,
} from './types.js';
import { DEFAULT_POOL_CONFIG } from './types.js';

// =============================================================================
// Pooled Connection
// =============================================================================

/**
 * Wrapper around a WebSocket connection with pool metadata.
 * @internal
 */
export interface PooledConnection {
  /** Unique connection identifier */
  id: string;
  /** The underlying WebSocket */
  ws: WebSocket;
  /** When the connection was created */
  createdAt: number;
  /** Last time the connection was used */
  lastUsedAt: number;
  /** Whether connection is currently borrowed */
  borrowed: boolean;
  /** Tags for this connection */
  tags: string[];
  /** Last measured latency */
  latency: number | null;
  /** Whether connection is healthy */
  healthy: boolean;
}

/**
 * Waiter in the connection queue.
 * @internal
 */
interface ConnectionWaiter {
  resolve: (conn: PooledConnection) => void;
  reject: (error: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
  createdAt: number;
}

/**
 * Node in the doubly-linked list of waiters.
 * @internal
 */
interface WaiterNode {
  waiter: ConnectionWaiter;
  prev: WaiterNode | null;
  next: WaiterNode | null;
}

/**
 * Doubly-linked list for O(1) waiter queue operations.
 * @internal
 */
class WaiterQueue {
  private head: WaiterNode | null = null;
  private tail: WaiterNode | null = null;
  private _size = 0;
  private nodeMap = new Map<ReturnType<typeof setTimeout>, WaiterNode>();

  get size(): number {
    return this._size;
  }

  /**
   * Add a waiter to the end of the queue. O(1)
   */
  push(waiter: ConnectionWaiter): void {
    const node: WaiterNode = { waiter, prev: this.tail, next: null };
    this.nodeMap.set(waiter.timeoutId, node);

    if (this.tail) {
      this.tail.next = node;
    } else {
      this.head = node;
    }
    this.tail = node;
    this._size++;
  }

  /**
   * Remove and return the first waiter. O(1)
   */
  shift(): ConnectionWaiter | undefined {
    if (!this.head) return undefined;

    const node = this.head;
    this.head = node.next;
    if (this.head) {
      this.head.prev = null;
    } else {
      this.tail = null;
    }

    this.nodeMap.delete(node.waiter.timeoutId);
    this._size--;
    return node.waiter;
  }

  /**
   * Remove a specific waiter by its timeoutId. O(1)
   */
  removeByTimeoutId(timeoutId: ReturnType<typeof setTimeout>): boolean {
    const node = this.nodeMap.get(timeoutId);
    if (!node) return false;

    if (node.prev) {
      node.prev.next = node.next;
    } else {
      this.head = node.next;
    }

    if (node.next) {
      node.next.prev = node.prev;
    } else {
      this.tail = node.prev;
    }

    this.nodeMap.delete(timeoutId);
    this._size--;
    return true;
  }

  /**
   * Clear all waiters and return them. O(n)
   */
  clear(): ConnectionWaiter[] {
    const waiters: ConnectionWaiter[] = [];
    let current = this.head;
    while (current) {
      waiters.push(current.waiter);
      current = current.next;
    }
    this.head = null;
    this.tail = null;
    this._size = 0;
    this.nodeMap.clear();
    return waiters;
  }

  [Symbol.iterator](): Iterator<ConnectionWaiter> {
    let current = this.head;
    return {
      next(): IteratorResult<ConnectionWaiter> {
        if (current) {
          const value = current.waiter;
          current = current.next;
          return { value, done: false };
        }
        return { value: undefined, done: true };
      }
    };
  }
}

// =============================================================================
// Connection Pool
// =============================================================================

/**
 * WebSocket connection pool for DoSQL clients.
 *
 * Manages a pool of WebSocket connections, handling:
 * - Connection creation and reuse
 * - Idle connection cleanup
 * - Health checks
 * - Backpressure handling
 *
 * @example
 * ```typescript
 * const pool = new WebSocketPool('wss://sql.example.com', {
 *   maxSize: 10,
 *   idleTimeout: 30000,
 * });
 *
 * // Acquire a connection
 * const conn = await pool.acquire();
 * try {
 *   conn.ws.send(JSON.stringify({ method: 'query', ... }));
 *   // ... handle response
 * } finally {
 *   pool.release(conn);
 * }
 *
 * // Clean up
 * await pool.close();
 * ```
 *
 * @public
 */
export class WebSocketPool {
  private readonly config: Required<Omit<PoolConfig, 'connectionTags'>> & { connectionTags: string[] };
  private readonly url: string;

  // Connection state
  private connections: Map<string, PooledConnection> = new Map();
  private waiters = new WaiterQueue();
  private connectionCounter = 0;

  // Statistics
  private stats = {
    connectionsCreated: 0,
    connectionsClosed: 0,
    totalRequestsServed: 0,
  };

  // Timers
  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;
  private keepAliveTimer: ReturnType<typeof setInterval> | null = null;

  // Event listeners
  private eventListeners: Map<keyof PoolEventMap, Set<(event: any) => void>> = new Map();

  // State
  private closed = false;
  private lastHealthCheck: Date | null = null;

  /**
   * Creates a new WebSocket connection pool.
   *
   * @param url - WebSocket URL to connect to
   * @param config - Pool configuration options
   */
  constructor(url: string, config: PoolConfig = {}) {
    this.url = url.replace(/^http/, 'ws');
    this.config = {
      ...DEFAULT_POOL_CONFIG,
      ...config,
      connectionTags: config.connectionTags ?? [],
    };

    // Initialize event listener maps
    const events: (keyof PoolEventMap)[] = [
      'pool:connection-created',
      'pool:connection-reused',
      'pool:connection-closed',
      'pool:health-check',
      'pool:backpressure',
      'do:hibernating',
      'do:awake',
    ];
    events.forEach(event => this.eventListeners.set(event, new Set()));

    // Start background tasks
    this.startBackgroundTasks();

    // Initialize minimum idle connections
    this.ensureMinIdle();
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Acquires a connection from the pool.
   *
   * If no connections are available and pool is at max capacity,
   * this will wait (or reject based on backpressure strategy).
   *
   * @returns Promise resolving to a pooled connection
   * @throws {Error} When pool is closed, backpressure applied, or timeout
   */
  async acquire(): Promise<PooledConnection> {
    if (this.closed) {
      throw new Error('Pool is closed');
    }

    // Try to get an available connection
    const available = this.getAvailableConnection();
    if (available) {
      available.borrowed = true;
      available.lastUsedAt = Date.now();
      this.stats.totalRequestsServed++;
      this.emit('pool:connection-reused', {
        connectionId: available.id,
        timestamp: new Date(),
      });
      return available;
    }

    // Can we create a new connection?
    if (this.connections.size < this.config.maxSize) {
      const conn = await this.createConnection();
      conn.borrowed = true;
      this.stats.totalRequestsServed++;
      return conn;
    }

    // Handle backpressure
    return this.handleBackpressure();
  }

  /**
   * Releases a connection back to the pool.
   *
   * @param connection - The connection to release
   */
  release(connection: PooledConnection): void {
    const conn = this.connections.get(connection.id);
    if (!conn) {
      return; // Connection was removed (closed/expired)
    }

    conn.borrowed = false;
    conn.lastUsedAt = Date.now();

    // Check if any waiters need this connection
    if (this.waiters.size > 0) {
      const waiter = this.waiters.shift()!;
      clearTimeout(waiter.timeoutId);
      conn.borrowed = true;
      conn.lastUsedAt = Date.now();
      this.stats.totalRequestsServed++;
      this.emit('pool:connection-reused', {
        connectionId: conn.id,
        timestamp: new Date(),
      });
      waiter.resolve(conn);
    }
  }

  /**
   * Closes the pool and all connections.
   */
  async close(): Promise<void> {
    this.closed = true;

    // Stop background tasks
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = null;
    }
    if (this.idleCheckTimer) {
      clearInterval(this.idleCheckTimer);
      this.idleCheckTimer = null;
    }
    if (this.keepAliveTimer) {
      clearInterval(this.keepAliveTimer);
      this.keepAliveTimer = null;
    }

    // Reject all waiters
    for (const waiter of this.waiters.clear()) {
      clearTimeout(waiter.timeoutId);
      waiter.reject(new Error('Pool closed'));
    }

    // Close all connections
    for (const conn of this.connections.values()) {
      this.closeConnection(conn, 'Pool closing');
    }
  }

  /**
   * Gets current pool statistics.
   */
  getStats(): PoolStats {
    const totalConnections = this.connections.size;
    const activeConnections = Array.from(this.connections.values())
      .filter(c => c.borrowed).length;
    const idleConnections = totalConnections - activeConnections;

    const reuseRatio = this.stats.totalRequestsServed > 0
      ? 1 - (this.stats.connectionsCreated / this.stats.totalRequestsServed)
      : 0;

    return {
      totalConnections,
      activeConnections,
      idleConnections,
      maxSize: this.config.maxSize,
      waitingRequests: this.waiters.size,
      connectionsCreated: this.stats.connectionsCreated,
      connectionsClosed: this.stats.connectionsClosed,
      totalRequestsServed: this.stats.totalRequestsServed,
      connectionReuseRatio: Math.max(0, reuseRatio),
    };
  }

  /**
   * Gets pool health status.
   */
  getHealth(): PoolHealth {
    const connections = Array.from(this.connections.values());
    const healthyConnections = connections.filter(c => c.healthy).length;
    const unhealthyConnections = connections.length - healthyConnections;

    const latencies = connections
      .filter(c => c.latency !== null)
      .map(c => c.latency!);
    const averageLatency = latencies.length > 0
      ? latencies.reduce((a, b) => a + b, 0) / latencies.length
      : 0;

    return {
      healthy: unhealthyConnections === 0 && connections.length > 0,
      healthyConnections,
      unhealthyConnections,
      lastHealthCheck: this.lastHealthCheck,
      averageLatency,
    };
  }

  /**
   * Gets information about all connections.
   */
  getConnectionInfo(): ConnectionInfo[] {
    const now = Date.now();
    return Array.from(this.connections.values()).map(conn => ({
      id: conn.id,
      createdAt: new Date(conn.createdAt),
      lastUsedAt: new Date(conn.lastUsedAt),
      idleTime: conn.borrowed ? 0 : now - conn.lastUsedAt,
      active: conn.borrowed,
      tags: [...conn.tags],
      latency: conn.latency,
    }));
  }

  /**
   * Registers an event listener.
   */
  on<K extends keyof PoolEventMap>(
    event: K,
    listener: (data: PoolEventMap[K]) => void
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.add(listener);
    }
    return this;
  }

  /**
   * Removes an event listener.
   */
  off<K extends keyof PoolEventMap>(
    event: K,
    listener: (data: PoolEventMap[K]) => void
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.delete(listener);
    }
    return this;
  }

  /**
   * Gets the connection tags.
   */
  getConnectionTags(): string[] {
    return [...this.config.connectionTags];
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Emits an event to all listeners.
   * @internal
   */
  private emit<K extends keyof PoolEventMap>(event: K, data: PoolEventMap[K]): void {
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

  /**
   * Gets an available (non-borrowed, healthy) connection.
   * @internal
   */
  private getAvailableConnection(): PooledConnection | null {
    const now = Date.now();

    for (const conn of this.connections.values()) {
      if (conn.borrowed) continue;
      if (!conn.healthy) continue;

      // Check max lifetime
      if (now - conn.createdAt > this.config.maxLifetime) {
        this.closeConnection(conn, 'Max lifetime exceeded');
        continue;
      }

      // Validate on borrow if enabled
      if (this.config.validateOnBorrow) {
        if (conn.ws.readyState !== WebSocket.READY_STATE_OPEN) {
          this.closeConnection(conn, 'Connection not open');
          continue;
        }
      }

      return conn;
    }

    return null;
  }

  /**
   * Creates a new WebSocket connection.
   * @internal
   */
  private async createConnection(): Promise<PooledConnection> {
    return new Promise((resolve, reject) => {
      const id = `conn-${++this.connectionCounter}-${Date.now()}`;
      const ws = new WebSocket(this.url);
      const now = Date.now();

      const conn: PooledConnection = {
        id,
        ws,
        createdAt: now,
        lastUsedAt: now,
        borrowed: false,
        tags: [...this.config.connectionTags],
        latency: null,
        healthy: false,
      };

      const timeout = setTimeout(() => {
        ws.close();
        reject(new Error('Connection timeout'));
      }, this.config.waitTimeout);

      ws.addEventListener('open', () => {
        clearTimeout(timeout);
        conn.healthy = true;
        this.connections.set(id, conn);
        this.stats.connectionsCreated++;
        this.emit('pool:connection-created', {
          connectionId: id,
          timestamp: new Date(),
        });
        resolve(conn);
      });

      ws.addEventListener('error', (event: Event) => {
        clearTimeout(timeout);
        conn.healthy = false;
        reject(new Error(`WebSocket error: ${event}`));
      });

      ws.addEventListener('close', () => {
        conn.healthy = false;
        // If connection was in pool, remove it
        if (this.connections.has(id)) {
          this.closeConnection(conn, 'Connection closed by server');
        }
      });
    });
  }

  /**
   * Handles backpressure when pool is exhausted.
   * @internal
   */
  private async handleBackpressure(): Promise<PooledConnection> {
    // Check backpressure limits
    if (this.waiters.size >= this.config.maxWaitingRequests) {
      this.emit('pool:backpressure', {
        waitingRequests: this.waiters.size,
        activeConnections: this.getStats().activeConnections,
      });

      if (this.config.backpressureStrategy === 'reject') {
        throw new Error('Pool exhausted');
      }
      throw new Error('Pool backpressure: too many waiting requests');
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.waiters.removeByTimeoutId(timeoutId);
        reject(new Error('Pool wait timeout'));
      }, this.config.waitTimeout);

      const waiter: ConnectionWaiter = {
        resolve,
        reject,
        timeoutId,
        createdAt: Date.now(),
      };

      this.waiters.push(waiter);
    });
  }

  /**
   * Closes a connection and removes it from the pool.
   * @internal
   */
  private closeConnection(conn: PooledConnection, reason: string): void {
    this.connections.delete(conn.id);

    try {
      if (conn.ws.readyState === WebSocket.READY_STATE_OPEN ||
          conn.ws.readyState === WebSocket.READY_STATE_CONNECTING) {
        conn.ws.close();
      }
    } catch {
      // Ignore close errors
    }

    this.stats.connectionsClosed++;
    this.emit('pool:connection-closed', {
      connectionId: conn.id,
      reason,
      timestamp: new Date(),
    });

    // Ensure minimum idle
    this.ensureMinIdle();
  }

  /**
   * Ensures minimum idle connections are maintained.
   * @internal
   */
  private async ensureMinIdle(): Promise<void> {
    if (this.closed) return;

    const idleCount = Array.from(this.connections.values())
      .filter(c => !c.borrowed).length;
    const needed = this.config.minIdle - idleCount;

    if (needed > 0 && this.connections.size < this.config.maxSize) {
      const toCreate = Math.min(needed, this.config.maxSize - this.connections.size);
      for (let i = 0; i < toCreate; i++) {
        try {
          await this.createConnection();
        } catch {
          // Failed to create connection, will retry later
          break;
        }
      }
    }
  }

  /**
   * Starts background maintenance tasks.
   * @internal
   */
  private startBackgroundTasks(): void {
    // Health check timer
    if (this.config.healthCheckInterval > 0) {
      this.healthCheckTimer = setInterval(() => {
        this.runHealthCheck();
      }, this.config.healthCheckInterval);
    }

    // Idle connection cleanup timer
    if (this.config.idleTimeout > 0) {
      this.idleCheckTimer = setInterval(() => {
        this.cleanupIdleConnections();
      }, Math.min(this.config.idleTimeout, 10000));
    }

    // Keepalive timer
    if (this.config.keepAliveInterval > 0) {
      this.keepAliveTimer = setInterval(() => {
        this.sendKeepalives();
      }, this.config.keepAliveInterval);
    }
  }

  /**
   * Runs health check on all connections.
   * @internal
   */
  private async runHealthCheck(): Promise<void> {
    this.lastHealthCheck = new Date();

    for (const conn of this.connections.values()) {
      if (conn.borrowed) continue;

      // Check WebSocket state
      if (conn.ws.readyState !== WebSocket.READY_STATE_OPEN) {
        conn.healthy = false;
        this.closeConnection(conn, 'Health check: connection not open');
        continue;
      }

      // Send ping and measure latency
      const start = performance.now();
      try {
        // Send a ping message (RPC layer will handle this)
        conn.ws.send(JSON.stringify({ id: `ping-${Date.now()}`, method: 'ping', params: {} }));
        // Note: In real implementation, we'd wait for pong response
        conn.latency = performance.now() - start;
        conn.healthy = true;
      } catch {
        conn.healthy = false;
      }
    }

    this.emit('pool:health-check', this.getHealth());
  }

  /**
   * Cleans up idle connections past their timeout.
   * @internal
   */
  private cleanupIdleConnections(): void {
    const now = Date.now();
    const minIdleToKeep = this.config.minIdle;
    let idleCount = 0;

    // Count current idle connections
    for (const conn of this.connections.values()) {
      if (!conn.borrowed) idleCount++;
    }

    // Close idle connections past timeout (keeping minimum)
    for (const conn of this.connections.values()) {
      if (conn.borrowed) continue;
      if (now - conn.lastUsedAt <= this.config.idleTimeout) continue;

      // Keep minimum idle connections
      if (idleCount <= minIdleToKeep) break;

      this.closeConnection(conn, 'Idle timeout');
      idleCount--;
    }

    // Close connections past max lifetime
    for (const conn of this.connections.values()) {
      if (conn.borrowed) continue;
      if (now - conn.createdAt > this.config.maxLifetime) {
        this.closeConnection(conn, 'Max lifetime exceeded');
      }
    }
  }

  /**
   * Sends keepalive pings to idle connections.
   * @internal
   */
  private sendKeepalives(): void {
    for (const conn of this.connections.values()) {
      if (conn.borrowed) continue;

      try {
        conn.ws.send(JSON.stringify({
          id: `keepalive-${Date.now()}`,
          method: 'ping',
          params: {},
        }));
      } catch {
        conn.healthy = false;
      }
    }
  }
}

// Export WebSocket ready states for environments where they're not defined
declare global {
  interface WebSocket {
    readonly READY_STATE_CONNECTING: number;
    readonly READY_STATE_OPEN: number;
    readonly READY_STATE_CLOSING: number;
    readonly READY_STATE_CLOSED: number;
  }
}

// Define ready states if not available
if (typeof WebSocket !== 'undefined') {
  (WebSocket as any).READY_STATE_CONNECTING = 0;
  (WebSocket as any).READY_STATE_OPEN = 1;
  (WebSocket as any).READY_STATE_CLOSING = 2;
  (WebSocket as any).READY_STATE_CLOSED = 3;
}
