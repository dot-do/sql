/**
 * Connection Pool for sql.do Client
 *
 * Provides enterprise-grade connection pooling with:
 * - Configurable min/max connections
 * - Connection health checking and automatic reconnection
 * - Request queuing when pool exhausted
 * - Connection timeout handling
 * - Graceful shutdown
 * - Pool warm-up on initialization
 * - Idle connection cleanup
 * - Connection affinity for transactions
 * - Metrics for pool utilization
 *
 * @packageDocumentation
 */

import type {
  PoolConfig,
  PoolStats,
  PoolHealth,
  ConnectionInfo,
  PoolEventMap,
  TransactionId,
} from './types.js';
import { DEFAULT_POOL_CONFIG } from './types.js';
import { ConnectionError } from './errors.js';
import { poolLogger } from './logger.js';

// =============================================================================
// Connection Pool Configuration
// =============================================================================

/**
 * Extended configuration for connection pool with additional options.
 * @public
 */
export interface ConnectionPoolConfig extends PoolConfig {
  /** URL to connect to */
  url: string;
  /** Whether to warm up the pool on initialization (default: false) */
  warmUp?: boolean;
  /** Number of connections to create during warm-up (default: minIdle) */
  warmUpSize?: number;
  /** Interval in ms for checking and replacing unhealthy connections (default: 15000) */
  healthCheckInterval?: number;
  /** Timeout for acquiring a connection from the pool (default: 30000) */
  acquireTimeout?: number;
  /** Maximum time in ms to wait for active queries during shutdown (default: 30000) */
  shutdownTimeout?: number;
}

/**
 * Default connection pool configuration values.
 * @internal
 */
const DEFAULT_CONNECTION_POOL_CONFIG: Required<Omit<ConnectionPoolConfig, 'url' | 'connectionTags'>> & {
  connectionTags: string[];
} = {
  ...DEFAULT_POOL_CONFIG,
  warmUp: false,
  warmUpSize: 0,
  healthCheckInterval: 15000,
  acquireTimeout: 30000,
  shutdownTimeout: 30000,
};

// =============================================================================
// Pooled Connection Types
// =============================================================================

/**
 * State of a pooled connection.
 * @internal
 */
export type ConnectionState = 'idle' | 'active' | 'unhealthy' | 'closing';

/**
 * A connection managed by the pool.
 * @internal
 */
export interface PooledConnection {
  /** Unique connection identifier */
  id: string;
  /** The underlying WebSocket */
  ws: WebSocket;
  /** Current state of the connection */
  state: ConnectionState;
  /** When the connection was created */
  createdAt: number;
  /** Last time the connection was used */
  lastUsedAt: number;
  /** Last measured latency in ms */
  latency: number | null;
  /** Number of requests served by this connection */
  requestCount: number;
  /** Transaction ID if connection is pinned to a transaction */
  transactionId?: TransactionId;
  /** Tags for this connection */
  tags: string[];
}

/**
 * Request waiting for a connection.
 * @internal
 */
interface WaitingRequest {
  resolve: (conn: PooledConnection) => void;
  reject: (error: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
  createdAt: number;
  transactionId?: TransactionId;
}

// =============================================================================
// Pool Events
// =============================================================================

/**
 * Event data for pool events.
 * @public
 */
export interface ConnectionPoolEventMap extends PoolEventMap {
  'pool:warm-up-complete': { connectionsCreated: number; duration: number };
  'pool:shutdown-started': { activeConnections: number };
  'pool:shutdown-complete': { duration: number; forcedClose: boolean };
  'pool:connection-replaced': { oldConnectionId: string; newConnectionId: string; reason: string };
}

/**
 * Event listener type for connection pool events.
 * @public
 */
export type ConnectionPoolEventListener<K extends keyof ConnectionPoolEventMap> = (
  event: ConnectionPoolEventMap[K]
) => void;

// =============================================================================
// Connection Pool Implementation
// =============================================================================

/**
 * Enterprise-grade connection pool for sql.do client.
 *
 * Manages a pool of WebSocket connections with:
 * - Automatic connection reuse across requests
 * - Health checking and automatic reconnection
 * - Transaction pinning for connection affinity
 * - Request queuing when pool is exhausted
 * - Graceful shutdown with query completion
 *
 * @example
 * ```typescript
 * const pool = new ConnectionPool({
 *   url: 'wss://sql.example.com',
 *   maxSize: 10,
 *   minIdle: 2,
 *   warmUp: true,
 * });
 *
 * await pool.initialize();
 *
 * // Acquire a connection
 * const conn = await pool.acquire();
 * try {
 *   // Use connection...
 * } finally {
 *   pool.release(conn);
 * }
 *
 * // For transactions, pin the connection
 * const txConn = await pool.acquire({ transactionId: 'tx-123' });
 * // All operations with this transactionId will use the same connection
 *
 * // Graceful shutdown
 * await pool.shutdown();
 * ```
 *
 * @public
 */
export class ConnectionPool {
  private readonly config: Required<Omit<ConnectionPoolConfig, 'connectionTags'>> & {
    connectionTags: string[];
  };
  private readonly url: string;

  // Connection state
  private connections: Map<string, PooledConnection> = new Map();
  private waitingRequests: WaitingRequest[] = [];
  private connectionCounter = 0;

  // Transaction affinity - maps transaction IDs to connection IDs
  private transactionConnections: Map<string, string> = new Map();

  // Pool state
  private initialized = false;
  private shuttingDown = false;
  private closed = false;

  // Statistics
  private stats = {
    connectionsCreated: 0,
    connectionsClosed: 0,
    connectionsReplaced: 0,
    totalRequestsServed: 0,
    totalWaitTime: 0,
    peakActiveConnections: 0,
  };

  // Timers
  private healthCheckTimer: ReturnType<typeof setInterval> | null = null;
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;
  private keepAliveTimer: ReturnType<typeof setInterval> | null = null;

  // Event listeners
  private eventListeners: Map<keyof ConnectionPoolEventMap, Set<(event: unknown) => void>> =
    new Map();

  // Last health check timestamp
  private lastHealthCheck: Date | null = null;

  /**
   * Creates a new connection pool.
   *
   * @param config - Pool configuration options
   */
  constructor(config: ConnectionPoolConfig) {
    this.url = config.url.replace(/^http/, 'ws');
    this.config = {
      ...DEFAULT_CONNECTION_POOL_CONFIG,
      ...config,
      url: this.url,
      connectionTags: config.connectionTags ?? [],
      warmUpSize: config.warmUpSize ?? config.minIdle ?? 0,
    };

    // Initialize event listener maps
    const events: (keyof ConnectionPoolEventMap)[] = [
      'pool:connection-created',
      'pool:connection-reused',
      'pool:connection-closed',
      'pool:health-check',
      'pool:backpressure',
      'pool:warm-up-complete',
      'pool:shutdown-started',
      'pool:shutdown-complete',
      'pool:connection-replaced',
      'do:hibernating',
      'do:awake',
    ];
    events.forEach((event) => this.eventListeners.set(event, new Set()));
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initializes the connection pool.
   *
   * If warmUp is enabled, creates initial connections up to warmUpSize.
   * Starts background health check and idle cleanup timers.
   *
   * @returns Promise that resolves when initialization is complete
   */
  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.initialized = true;

    // Start background tasks
    this.startBackgroundTasks();

    // Warm up pool if configured
    if (this.config.warmUp && this.config.warmUpSize > 0) {
      await this.warmUp();
    } else {
      // Ensure minimum idle connections
      await this.ensureMinIdle();
    }
  }

  /**
   * Warms up the pool by creating initial connections.
   * @internal
   */
  private async warmUp(): Promise<void> {
    const startTime = Date.now();
    const targetSize = Math.min(this.config.warmUpSize, this.config.maxSize);
    let created = 0;

    poolLogger.info('Pool warm-up started', {
      targetSize,
      url: this.url,
    });

    const createPromises: Promise<void>[] = [];
    for (let i = 0; i < targetSize; i++) {
      createPromises.push(
        this.createConnection()
          .then(() => {
            created++;
          })
          .catch((error) => {
            poolLogger.warn('Failed to create connection during warm-up', {
              error: error instanceof Error ? error.message : String(error),
            });
          })
      );
    }

    await Promise.all(createPromises);

    const duration = Date.now() - startTime;
    poolLogger.info('Pool warm-up complete', {
      connectionsCreated: created,
      duration,
    });

    this.emit('pool:warm-up-complete', {
      connectionsCreated: created,
      duration,
    });
  }

  // ===========================================================================
  // Connection Acquisition
  // ===========================================================================

  /**
   * Acquires a connection from the pool.
   *
   * If a transactionId is provided, returns the connection pinned to that
   * transaction, or acquires a new connection and pins it.
   *
   * @param options - Optional acquisition options
   * @returns Promise resolving to a pooled connection
   * @throws {ConnectionError} When pool is closed or acquisition times out
   */
  async acquire(options?: { transactionId?: TransactionId }): Promise<PooledConnection> {
    if (this.closed) {
      throw new ConnectionError('Pool is closed', this.url);
    }

    if (this.shuttingDown) {
      throw new ConnectionError('Pool is shutting down', this.url);
    }

    // Check for transaction affinity
    if (options?.transactionId) {
      const existingConnId = this.transactionConnections.get(String(options.transactionId));
      if (existingConnId) {
        const conn = this.connections.get(existingConnId);
        if (conn && conn.state === 'active' && conn.ws.readyState === WebSocket.OPEN) {
          conn.lastUsedAt = Date.now();
          return conn;
        }
        // Connection is gone or unhealthy, remove mapping
        this.transactionConnections.delete(String(options.transactionId));
      }
    }

    // Try to get an available connection
    const available = this.getAvailableConnection();
    if (available) {
      return this.borrowConnection(available, options?.transactionId);
    }

    // Can we create a new connection?
    if (this.connections.size < this.config.maxSize) {
      try {
        const conn = await this.createConnection();
        return this.borrowConnection(conn, options?.transactionId);
      } catch (error) {
        // Fall through to queuing
        poolLogger.warn('Failed to create new connection, queuing request', {
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    // Queue the request
    return this.waitForConnection(options?.transactionId);
  }

  /**
   * Gets an available (idle, healthy) connection.
   * @internal
   */
  private getAvailableConnection(): PooledConnection | null {
    const now = Date.now();

    for (const conn of this.connections.values()) {
      if (conn.state !== 'idle') continue;
      if (conn.ws.readyState !== WebSocket.OPEN) continue;

      // Check max lifetime
      if (now - conn.createdAt > this.config.maxLifetime) {
        this.closeConnection(conn, 'Max lifetime exceeded');
        continue;
      }

      // Validate on borrow if enabled
      if (this.config.validateOnBorrow && !this.isConnectionHealthy(conn)) {
        this.markUnhealthy(conn);
        continue;
      }

      return conn;
    }

    return null;
  }

  /**
   * Borrows a connection, marking it as active.
   * @internal
   */
  private borrowConnection(
    conn: PooledConnection,
    transactionId?: TransactionId
  ): PooledConnection {
    conn.state = 'active';
    conn.lastUsedAt = Date.now();
    conn.requestCount++;

    if (transactionId) {
      conn.transactionId = transactionId;
      this.transactionConnections.set(String(transactionId), conn.id);
    }

    this.stats.totalRequestsServed++;
    this.updatePeakActiveConnections();

    this.emit('pool:connection-reused', {
      connectionId: conn.id,
      timestamp: new Date(),
    });

    return conn;
  }

  /**
   * Waits for a connection to become available.
   * @internal
   */
  private waitForConnection(transactionId?: TransactionId): Promise<PooledConnection> {
    // Check backpressure limits
    if (this.waitingRequests.length >= this.config.maxWaitingRequests) {
      this.emit('pool:backpressure', {
        waitingRequests: this.waitingRequests.length,
        activeConnections: this.getActiveConnectionCount(),
      });

      if (this.config.backpressureStrategy === 'reject') {
        throw new ConnectionError('Pool exhausted', this.url);
      }
      throw new ConnectionError('Pool backpressure: too many waiting requests', this.url);
    }

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const index = this.waitingRequests.findIndex((r) => r.timeoutId === timeoutId);
        if (index !== -1) {
          this.waitingRequests.splice(index, 1);
        }
        reject(new ConnectionError('Connection acquisition timeout', this.url));
      }, this.config.acquireTimeout);

      const request: WaitingRequest = {
        resolve,
        reject,
        timeoutId,
        createdAt: Date.now(),
        transactionId,
      };

      this.waitingRequests.push(request);
    });
  }

  // ===========================================================================
  // Connection Release
  // ===========================================================================

  /**
   * Releases a connection back to the pool.
   *
   * If the connection was pinned to a transaction and transactionComplete is true,
   * the transaction affinity is removed.
   *
   * @param connection - The connection to release
   * @param options - Optional release options
   */
  release(connection: PooledConnection, options?: { transactionComplete?: boolean }): void {
    const conn = this.connections.get(connection.id);
    if (!conn) {
      return; // Connection was removed
    }

    // Clear transaction affinity if transaction is complete
    if (options?.transactionComplete && conn.transactionId) {
      this.transactionConnections.delete(String(conn.transactionId));
      conn.transactionId = undefined;
    }

    // If connection is still pinned to a transaction, keep it active
    if (conn.transactionId) {
      return;
    }

    conn.state = 'idle';
    conn.lastUsedAt = Date.now();

    // Check if any waiters need this connection
    this.processWaitingRequests();
  }

  /**
   * Processes waiting requests with available connections.
   * @internal
   */
  private processWaitingRequests(): void {
    while (this.waitingRequests.length > 0) {
      const conn = this.getAvailableConnection();
      if (!conn) break;

      const waiter = this.waitingRequests.shift()!;
      clearTimeout(waiter.timeoutId);

      // Track wait time
      this.stats.totalWaitTime += Date.now() - waiter.createdAt;

      try {
        const borrowed = this.borrowConnection(conn, waiter.transactionId);
        waiter.resolve(borrowed);
      } catch (error) {
        waiter.reject(error as Error);
      }
    }
  }

  // ===========================================================================
  // Connection Creation
  // ===========================================================================

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
        state: 'idle',
        createdAt: now,
        lastUsedAt: now,
        latency: null,
        requestCount: 0,
        tags: [...this.config.connectionTags],
      };

      const timeout = setTimeout(() => {
        try {
          ws.close();
        } catch {
          // Ignore close errors
        }
        reject(new ConnectionError('Connection timeout', this.url));
      }, this.config.waitTimeout);

      ws.addEventListener('open', () => {
        clearTimeout(timeout);
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
        reject(new ConnectionError(`WebSocket error: ${event}`, this.url));
      });

      ws.addEventListener('close', () => {
        if (this.connections.has(id)) {
          this.handleConnectionClose(conn);
        }
      });
    });
  }

  /**
   * Handles connection close event.
   * @internal
   */
  private handleConnectionClose(conn: PooledConnection): void {
    // Clean up transaction affinity
    if (conn.transactionId) {
      this.transactionConnections.delete(String(conn.transactionId));
    }

    this.connections.delete(conn.id);
    this.stats.connectionsClosed++;

    this.emit('pool:connection-closed', {
      connectionId: conn.id,
      reason: 'Connection closed by server',
      timestamp: new Date(),
    });

    // Ensure minimum idle connections (unless shutting down)
    if (!this.shuttingDown && !this.closed) {
      this.ensureMinIdle().catch((error) => {
        poolLogger.error(
          'Failed to ensure minimum idle connections',
          error instanceof Error ? error : new Error(String(error))
        );
      });
    }
  }

  // ===========================================================================
  // Connection Health
  // ===========================================================================

  /**
   * Checks if a connection is healthy.
   * @internal
   */
  private isConnectionHealthy(conn: PooledConnection): boolean {
    if (conn.ws.readyState !== WebSocket.OPEN) {
      return false;
    }

    // Check max lifetime
    if (Date.now() - conn.createdAt > this.config.maxLifetime) {
      return false;
    }

    return true;
  }

  /**
   * Marks a connection as unhealthy and schedules replacement.
   * @internal
   */
  private markUnhealthy(conn: PooledConnection): void {
    conn.state = 'unhealthy';

    // If connection was pinned to a transaction, clear the mapping
    if (conn.transactionId) {
      this.transactionConnections.delete(String(conn.transactionId));
    }

    // Close and replace
    this.replaceConnection(conn, 'Connection unhealthy');
  }

  /**
   * Replaces an unhealthy connection with a new one.
   * @internal
   */
  private async replaceConnection(oldConn: PooledConnection, reason: string): Promise<void> {
    this.closeConnection(oldConn, reason);

    // Create replacement if below max size
    if (this.connections.size < this.config.maxSize) {
      try {
        const newConn = await this.createConnection();
        this.stats.connectionsReplaced++;

        this.emit('pool:connection-replaced', {
          oldConnectionId: oldConn.id,
          newConnectionId: newConn.id,
          reason,
        });
      } catch (error) {
        poolLogger.warn('Failed to create replacement connection', {
          error: error instanceof Error ? error.message : String(error),
          reason,
        });
      }
    }
  }

  /**
   * Closes a connection and removes it from the pool.
   * @internal
   */
  private closeConnection(conn: PooledConnection, reason: string): void {
    conn.state = 'closing';
    this.connections.delete(conn.id);

    // Clear transaction affinity
    if (conn.transactionId) {
      this.transactionConnections.delete(String(conn.transactionId));
    }

    try {
      if (
        conn.ws.readyState === WebSocket.OPEN ||
        conn.ws.readyState === WebSocket.CONNECTING
      ) {
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
  }

  // ===========================================================================
  // Background Tasks
  // ===========================================================================

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
      this.idleCheckTimer = setInterval(
        () => {
          this.cleanupIdleConnections();
        },
        Math.min(this.config.idleTimeout / 2, 10000)
      );
    }

    // Keepalive timer
    if (this.config.keepAliveInterval > 0) {
      this.keepAliveTimer = setInterval(() => {
        this.sendKeepalives();
      }, this.config.keepAliveInterval);
    }
  }

  /**
   * Stops all background tasks.
   * @internal
   */
  private stopBackgroundTasks(): void {
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
  }

  /**
   * Runs health check on all idle connections.
   * @internal
   */
  private runHealthCheck(): void {
    this.lastHealthCheck = new Date();

    for (const conn of this.connections.values()) {
      // Skip active connections
      if (conn.state === 'active') continue;

      if (!this.isConnectionHealthy(conn)) {
        this.markUnhealthy(conn);
        continue;
      }

      // Send ping and measure latency
      const start = performance.now();
      try {
        conn.ws.send(
          JSON.stringify({
            id: `ping-${Date.now()}`,
            method: 'ping',
            params: {},
          })
        );
        conn.latency = performance.now() - start;
      } catch {
        this.markUnhealthy(conn);
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
    let idleCount = this.getIdleConnectionCount();

    for (const conn of this.connections.values()) {
      if (conn.state !== 'idle') continue;

      // Keep minimum idle connections
      if (idleCount <= this.config.minIdle) break;

      // Check idle timeout
      if (now - conn.lastUsedAt > this.config.idleTimeout) {
        this.closeConnection(conn, 'Idle timeout');
        idleCount--;
      }

      // Check max lifetime
      if (now - conn.createdAt > this.config.maxLifetime) {
        this.closeConnection(conn, 'Max lifetime exceeded');
        idleCount--;
      }
    }

    // Ensure minimum idle connections
    this.ensureMinIdle().catch((error) => {
      poolLogger.error(
        'Failed to ensure minimum idle connections after cleanup',
        error instanceof Error ? error : new Error(String(error))
      );
    });
  }

  /**
   * Sends keepalive pings to idle connections.
   * @internal
   */
  private sendKeepalives(): void {
    for (const conn of this.connections.values()) {
      if (conn.state !== 'idle') continue;

      try {
        conn.ws.send(
          JSON.stringify({
            id: `keepalive-${Date.now()}`,
            method: 'ping',
            params: {},
          })
        );
      } catch {
        this.markUnhealthy(conn);
      }
    }
  }

  /**
   * Ensures minimum idle connections are maintained.
   * @internal
   */
  private async ensureMinIdle(): Promise<void> {
    if (this.closed || this.shuttingDown) return;

    const idleCount = this.getIdleConnectionCount();
    const needed = this.config.minIdle - idleCount;

    if (needed > 0 && this.connections.size < this.config.maxSize) {
      const toCreate = Math.min(needed, this.config.maxSize - this.connections.size);
      const createPromises: Promise<void>[] = [];

      for (let i = 0; i < toCreate; i++) {
        createPromises.push(
          this.createConnection().catch((error) => {
            poolLogger.warn('Failed to create idle connection', {
              error: error instanceof Error ? error.message : String(error),
            });
          })
        );
      }

      await Promise.all(createPromises);
    }
  }

  // ===========================================================================
  // Shutdown
  // ===========================================================================

  /**
   * Gracefully shuts down the pool.
   *
   * Waits for active queries to complete up to shutdownTimeout.
   * After timeout, forcibly closes remaining connections.
   *
   * @returns Promise that resolves when shutdown is complete
   */
  async shutdown(): Promise<void> {
    if (this.closed) return;

    this.shuttingDown = true;
    const startTime = Date.now();
    const activeCount = this.getActiveConnectionCount();

    poolLogger.info('Pool shutdown started', {
      activeConnections: activeCount,
      waitingRequests: this.waitingRequests.length,
    });

    this.emit('pool:shutdown-started', {
      activeConnections: activeCount,
    });

    // Stop background tasks
    this.stopBackgroundTasks();

    // Reject all waiting requests
    for (const waiter of this.waitingRequests) {
      clearTimeout(waiter.timeoutId);
      waiter.reject(new ConnectionError('Pool shutting down', this.url));
    }
    this.waitingRequests = [];

    // Wait for active connections to complete
    const forcedClose = await this.waitForActiveConnections();

    // Close all remaining connections
    for (const conn of this.connections.values()) {
      this.closeConnection(conn, 'Pool shutdown');
    }

    this.closed = true;
    this.shuttingDown = false;

    const duration = Date.now() - startTime;

    poolLogger.info('Pool shutdown complete', {
      duration,
      forcedClose,
    });

    this.emit('pool:shutdown-complete', {
      duration,
      forcedClose,
    });
  }

  /**
   * Waits for active connections to be released.
   * @returns true if shutdown was forced due to timeout
   * @internal
   */
  private async waitForActiveConnections(): Promise<boolean> {
    const startTime = Date.now();

    while (this.getActiveConnectionCount() > 0) {
      if (Date.now() - startTime > this.config.shutdownTimeout) {
        poolLogger.warn('Shutdown timeout reached, forcing connection close', {
          activeConnections: this.getActiveConnectionCount(),
        });
        return true;
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    return false;
  }

  /**
   * Alias for shutdown() for compatibility.
   */
  async close(): Promise<void> {
    return this.shutdown();
  }

  // ===========================================================================
  // Statistics and Info
  // ===========================================================================

  /**
   * Gets current pool statistics.
   */
  getStats(): PoolStats {
    const totalConnections = this.connections.size;
    const activeConnections = this.getActiveConnectionCount();
    const idleConnections = this.getIdleConnectionCount();

    const reuseRatio =
      this.stats.totalRequestsServed > 0
        ? 1 - this.stats.connectionsCreated / this.stats.totalRequestsServed
        : 0;

    return {
      totalConnections,
      activeConnections,
      idleConnections,
      maxSize: this.config.maxSize,
      waitingRequests: this.waitingRequests.length,
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
    const healthyConnections = connections.filter(
      (c) => c.state !== 'unhealthy' && c.ws.readyState === WebSocket.OPEN
    ).length;
    const unhealthyConnections = connections.length - healthyConnections;

    const latencies = connections.filter((c) => c.latency !== null).map((c) => c.latency!);
    const averageLatency =
      latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0;

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
    return Array.from(this.connections.values()).map((conn) => ({
      id: conn.id,
      createdAt: new Date(conn.createdAt),
      lastUsedAt: new Date(conn.lastUsedAt),
      idleTime: conn.state === 'idle' ? now - conn.lastUsedAt : 0,
      active: conn.state === 'active',
      tags: [...conn.tags],
      latency: conn.latency,
    }));
  }

  /**
   * Gets the connection tags.
   */
  getConnectionTags(): string[] {
    return [...this.config.connectionTags];
  }

  /**
   * Checks if the pool is initialized.
   */
  isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * Checks if the pool is closed.
   */
  isClosed(): boolean {
    return this.closed;
  }

  /**
   * Checks if the pool is shutting down.
   */
  isShuttingDown(): boolean {
    return this.shuttingDown;
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Gets count of active connections.
   * @internal
   */
  private getActiveConnectionCount(): number {
    return Array.from(this.connections.values()).filter((c) => c.state === 'active').length;
  }

  /**
   * Gets count of idle connections.
   * @internal
   */
  private getIdleConnectionCount(): number {
    return Array.from(this.connections.values()).filter((c) => c.state === 'idle').length;
  }

  /**
   * Updates peak active connections statistic.
   * @internal
   */
  private updatePeakActiveConnections(): void {
    const current = this.getActiveConnectionCount();
    if (current > this.stats.peakActiveConnections) {
      this.stats.peakActiveConnections = current;
    }
  }

  // ===========================================================================
  // Event Handling
  // ===========================================================================

  /**
   * Registers an event listener.
   */
  on<K extends keyof ConnectionPoolEventMap>(
    event: K,
    listener: ConnectionPoolEventListener<K>
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.add(listener as (event: unknown) => void);
    }
    return this;
  }

  /**
   * Removes an event listener.
   */
  off<K extends keyof ConnectionPoolEventMap>(
    event: K,
    listener: ConnectionPoolEventListener<K>
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.delete(listener as (event: unknown) => void);
    }
    return this;
  }

  /**
   * Emits an event to all listeners.
   * @internal
   */
  private emit<K extends keyof ConnectionPoolEventMap>(
    event: K,
    data: ConnectionPoolEventMap[K]
  ): void {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      for (const listener of listeners) {
        try {
          listener(data);
        } catch (error) {
          poolLogger.error(
            `Error in ${event} listener`,
            error instanceof Error ? error : new Error(String(error)),
            { event }
          );
        }
      }
    }
  }
}

// Define WebSocket constants if not available
declare global {
  interface WebSocket {
    readonly CONNECTING: number;
    readonly OPEN: number;
    readonly CLOSING: number;
    readonly CLOSED: number;
  }
}
