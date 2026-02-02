/**
 * DoSQL RPC Connection Pool
 *
 * Provides connection pooling for cross-DO RPC communication with:
 * - DO stub caching and reuse
 * - Configurable pool size per target DO
 * - Idle connection timeout and cleanup
 * - Health tracking per connection
 * - Statistics and monitoring
 *
 * In Cloudflare Workers, DO stubs are lightweight handles to Durable Objects,
 * but caching them provides benefits:
 * - Reduces repeated idFromName() calls
 * - Enables health tracking across requests
 * - Allows for connection-level statistics
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Configuration for the DO connection pool
 */
export interface DOConnectionPoolConfig {
  /** Maximum connections per DO ID (default: 10) */
  maxConnectionsPerDO: number;
  /** Maximum total connections across all DOs (default: 100) */
  maxTotalConnections: number;
  /** Idle timeout in ms before connection is evicted (default: 60000) */
  idleTimeoutMs: number;
  /** Health check interval in ms (default: 30000, 0 to disable) */
  healthCheckIntervalMs: number;
  /** Maximum consecutive failures before marking connection unhealthy (default: 3) */
  maxConsecutiveFailures: number;
  /** Time to wait before retrying an unhealthy connection (default: 10000) */
  unhealthyRetryMs: number;
  /** Whether to enable statistics tracking (default: true) */
  enableStats: boolean;
}

/**
 * Default pool configuration
 */
export const DEFAULT_DO_CONNECTION_POOL_CONFIG: DOConnectionPoolConfig = {
  maxConnectionsPerDO: 10,
  maxTotalConnections: 100,
  idleTimeoutMs: 60000,
  healthCheckIntervalMs: 30000,
  maxConsecutiveFailures: 3,
  unhealthyRetryMs: 10000,
  enableStats: true,
};

/**
 * Health status for a pooled connection
 */
export type ConnectionHealth = 'healthy' | 'unhealthy' | 'unknown';

/**
 * A pooled connection to a Durable Object
 */
export interface PooledConnection<T = unknown> {
  /** Unique connection ID */
  id: string;
  /** Target DO ID (the name or hex ID used to get the stub) */
  doId: string;
  /** DO namespace name (for multi-namespace pools) */
  namespace: string;
  /** The DO stub */
  stub: T;
  /** When the connection was created */
  createdAt: number;
  /** Last time the connection was used */
  lastUsedAt: number;
  /** Last time the connection was successfully used */
  lastSuccessAt?: number;
  /** Last time the connection failed */
  lastFailureAt?: number;
  /** Current health status */
  health: ConnectionHealth;
  /** Consecutive failure count */
  consecutiveFailures: number;
  /** Total requests made through this connection */
  totalRequests: number;
  /** Total successful requests */
  successfulRequests: number;
  /** Total failed requests */
  failedRequests: number;
  /** Whether the connection is currently in use */
  inUse: boolean;
}

/**
 * Options for acquiring a connection
 */
export interface AcquireOptions {
  /** Timeout for acquiring a connection (ms) */
  timeoutMs?: number;
  /** Whether to prefer healthy connections (default: true) */
  preferHealthy?: boolean;
  /** Whether to create a new connection if none available (default: true) */
  createIfNeeded?: boolean;
}

/**
 * Statistics for the connection pool
 */
export interface DOConnectionPoolStats {
  /** Total connections in pool */
  totalConnections: number;
  /** Connections currently in use */
  inUseConnections: number;
  /** Idle connections available */
  idleConnections: number;
  /** Healthy connections */
  healthyConnections: number;
  /** Unhealthy connections */
  unhealthyConnections: number;
  /** Connections by namespace */
  connectionsByNamespace: Map<string, number>;
  /** Total requests made through pool */
  totalRequests: number;
  /** Total successful requests */
  successfulRequests: number;
  /** Total failed requests */
  failedRequests: number;
  /** Connection acquisitions */
  acquisitions: number;
  /** Connection releases */
  releases: number;
  /** Connection evictions (due to idle timeout) */
  evictions: number;
  /** Connections created */
  connectionsCreated: number;
  /** Average connection age (ms) */
  averageConnectionAge: number;
  /** Pool hit rate (reused vs new connections) */
  hitRate: number;
}

/**
 * Factory function type for creating DO stubs
 *
 * @example Cloudflare Workers usage
 * ```typescript
 * const stubFactory: DOStubFactory = (namespace, doId) => {
 *   const doNamespace = env[namespace] as DurableObjectNamespace;
 *   const id = doNamespace.idFromName(doId);
 *   return doNamespace.get(id);
 * };
 * ```
 */
export type DOStubFactory<T = unknown> = (namespace: string, doId: string) => T;

/**
 * Event types emitted by the pool
 */
export interface DOConnectionPoolEvents {
  'connection:created': { connectionId: string; doId: string; namespace: string };
  'connection:acquired': { connectionId: string; doId: string };
  'connection:released': { connectionId: string; doId: string };
  'connection:evicted': { connectionId: string; doId: string; reason: string };
  'connection:healthChange': { connectionId: string; doId: string; oldHealth: ConnectionHealth; newHealth: ConnectionHealth };
  'pool:exhausted': { namespace: string; doId: string };
}

type PoolEventListener<K extends keyof DOConnectionPoolEvents> = (event: DOConnectionPoolEvents[K]) => void;

// =============================================================================
// Internal Types
// =============================================================================

/**
 * Internal connection representation with additional tracking
 * @internal
 */
interface InternalConnection<T> extends PooledConnection<T> {
  /** Timer for idle timeout */
  idleTimer?: ReturnType<typeof setTimeout>;
}

/**
 * Waiter for a connection
 * @internal
 */
interface ConnectionWaiter<T> {
  resolve: (connection: PooledConnection<T>) => void;
  reject: (error: Error) => void;
  timeoutId: ReturnType<typeof setTimeout>;
  doId: string;
  namespace: string;
  options: AcquireOptions;
}

// =============================================================================
// Connection Pool Implementation
// =============================================================================

/**
 * Connection pool for cross-DO RPC communication
 *
 * Manages a pool of DO stub connections with:
 * - Connection reuse to reduce overhead
 * - Idle timeout for automatic cleanup
 * - Health tracking per connection
 * - Statistics for monitoring
 *
 * @example Basic usage
 * ```typescript
 * // Create pool with stub factory
 * const pool = new DOConnectionPool((namespace, doId) => {
 *   const ns = env[namespace] as DurableObjectNamespace;
 *   return ns.get(ns.idFromName(doId));
 * });
 *
 * // Acquire a connection
 * const conn = await pool.acquire('USER_DO', 'user-123');
 *
 * try {
 *   // Use the connection
 *   const response = await conn.stub.fetch('/query', { method: 'POST', body: '...' });
 *   pool.recordSuccess(conn.id);
 * } catch (error) {
 *   pool.recordFailure(conn.id);
 *   throw error;
 * } finally {
 *   // Release back to pool
 *   pool.release(conn.id);
 * }
 * ```
 *
 * @example With wrapper helper
 * ```typescript
 * const result = await pool.withConnection('USER_DO', 'user-123', async (stub) => {
 *   const response = await stub.fetch('/query', { method: 'POST', body: '...' });
 *   return response.json();
 * });
 * ```
 */
export class DOConnectionPool<T = unknown> {
  private readonly config: DOConnectionPoolConfig;
  private readonly stubFactory: DOStubFactory<T>;

  // Connection storage: Map<namespace:doId, Map<connectionId, connection>>
  private readonly connections: Map<string, Map<string, InternalConnection<T>>> = new Map();

  // Waiters for connections when pool is exhausted
  private readonly waiters: Map<string, ConnectionWaiter<T>[]> = new Map();

  // Statistics
  private stats = {
    totalRequests: 0,
    successfulRequests: 0,
    failedRequests: 0,
    acquisitions: 0,
    releases: 0,
    evictions: 0,
    connectionsCreated: 0,
    hits: 0,
    misses: 0,
  };

  // Event listeners
  private readonly eventListeners: Map<keyof DOConnectionPoolEvents, Set<PoolEventListener<keyof DOConnectionPoolEvents>>> = new Map();

  // Timers
  private healthCheckTimer?: ReturnType<typeof setInterval>;

  // State
  private closed = false;
  private connectionCounter = 0;

  /**
   * Creates a new DO connection pool
   *
   * @param stubFactory - Factory function to create DO stubs
   * @param config - Pool configuration
   */
  constructor(stubFactory: DOStubFactory<T>, config: Partial<DOConnectionPoolConfig> = {}) {
    this.stubFactory = stubFactory;
    this.config = { ...DEFAULT_DO_CONNECTION_POOL_CONFIG, ...config };

    // Initialize event listeners
    const events: (keyof DOConnectionPoolEvents)[] = [
      'connection:created',
      'connection:acquired',
      'connection:released',
      'connection:evicted',
      'connection:healthChange',
      'pool:exhausted',
    ];
    events.forEach(event => this.eventListeners.set(event, new Set()));

    // Start health check timer if enabled
    if (this.config.healthCheckIntervalMs > 0) {
      this.healthCheckTimer = setInterval(() => {
        this.runHealthCheck();
      }, this.config.healthCheckIntervalMs);
    }
  }

  // ===========================================================================
  // Public API - Connection Management
  // ===========================================================================

  /**
   * Acquires a connection from the pool
   *
   * If a healthy idle connection exists, it's returned immediately.
   * Otherwise, a new connection is created (if under limits) or
   * the call waits for a connection to become available.
   *
   * @param namespace - DO namespace name
   * @param doId - DO identifier (name or hex ID)
   * @param options - Acquisition options
   * @returns A pooled connection
   * @throws {Error} When pool is closed, exhausted, or timeout
   */
  async acquire(
    namespace: string,
    doId: string,
    options: AcquireOptions = {}
  ): Promise<PooledConnection<T>> {
    if (this.closed) {
      throw new Error('Connection pool is closed');
    }

    const {
      timeoutMs = 30000,
      preferHealthy = true,
      createIfNeeded = true,
    } = options;

    const key = this.getKey(namespace, doId);
    this.stats.acquisitions++;

    // Try to get an existing idle connection
    const existingConn = this.getIdleConnection(key, preferHealthy);
    if (existingConn) {
      this.markInUse(existingConn);
      this.stats.hits++;
      this.emit('connection:acquired', { connectionId: existingConn.id, doId });
      return this.toPublicConnection(existingConn);
    }

    // Check if we can create a new connection
    const doConnections = this.connections.get(key);
    const currentCount = doConnections?.size ?? 0;
    const totalCount = this.getTotalConnectionCount();

    if (
      createIfNeeded &&
      currentCount < this.config.maxConnectionsPerDO &&
      totalCount < this.config.maxTotalConnections
    ) {
      const newConn = this.createConnection(namespace, doId);
      this.stats.misses++;
      this.emit('connection:acquired', { connectionId: newConn.id, doId });
      return this.toPublicConnection(newConn);
    }

    // Pool is exhausted - wait or reject
    this.emit('pool:exhausted', { namespace, doId });

    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.removeWaiter(key, waiter);
        reject(new Error(`Connection acquisition timeout after ${timeoutMs}ms for ${namespace}:${doId}`));
      }, timeoutMs);

      const waiter: ConnectionWaiter<T> = {
        resolve: (conn) => {
          clearTimeout(timeoutId);
          resolve(conn);
        },
        reject: (err) => {
          clearTimeout(timeoutId);
          reject(err);
        },
        timeoutId,
        doId,
        namespace,
        options,
      };

      this.addWaiter(key, waiter);
    });
  }

  /**
   * Releases a connection back to the pool
   *
   * @param connectionId - ID of the connection to release
   */
  release(connectionId: string): void {
    const conn = this.findConnectionById(connectionId);
    if (!conn) return;

    conn.inUse = false;
    conn.lastUsedAt = Date.now();
    this.stats.releases++;

    // Reset idle timer
    this.resetIdleTimer(conn);

    this.emit('connection:released', { connectionId, doId: conn.doId });

    // Check if any waiters need this connection
    const key = this.getKey(conn.namespace, conn.doId);
    this.serveWaiters(key);
  }

  /**
   * Records a successful request on a connection
   *
   * @param connectionId - Connection ID
   */
  recordSuccess(connectionId: string): void {
    const conn = this.findConnectionById(connectionId);
    if (!conn) return;

    const now = Date.now();
    conn.lastSuccessAt = now;
    conn.totalRequests++;
    conn.successfulRequests++;
    conn.consecutiveFailures = 0;

    if (this.config.enableStats) {
      this.stats.totalRequests++;
      this.stats.successfulRequests++;
    }

    // Update health if was unhealthy
    if (conn.health !== 'healthy') {
      this.updateConnectionHealth(conn, 'healthy');
    }
  }

  /**
   * Records a failed request on a connection
   *
   * @param connectionId - Connection ID
   */
  recordFailure(connectionId: string): void {
    const conn = this.findConnectionById(connectionId);
    if (!conn) return;

    const now = Date.now();
    conn.lastFailureAt = now;
    conn.totalRequests++;
    conn.failedRequests++;
    conn.consecutiveFailures++;

    if (this.config.enableStats) {
      this.stats.totalRequests++;
      this.stats.failedRequests++;
    }

    // Mark unhealthy if too many consecutive failures
    if (conn.consecutiveFailures >= this.config.maxConsecutiveFailures) {
      this.updateConnectionHealth(conn, 'unhealthy');
    }
  }

  /**
   * Executes a function with a pooled connection
   *
   * Automatically handles acquire, success/failure tracking, and release.
   *
   * @param namespace - DO namespace name
   * @param doId - DO identifier
   * @param fn - Function to execute with the stub
   * @param options - Acquisition options
   * @returns Result of the function
   */
  async withConnection<R>(
    namespace: string,
    doId: string,
    fn: (stub: T) => Promise<R>,
    options?: AcquireOptions
  ): Promise<R> {
    const conn = await this.acquire(namespace, doId, options);

    try {
      const result = await fn(conn.stub);
      this.recordSuccess(conn.id);
      return result;
    } catch (error) {
      this.recordFailure(conn.id);
      throw error;
    } finally {
      this.release(conn.id);
    }
  }

  /**
   * Removes a specific connection from the pool
   *
   * @param connectionId - Connection ID to remove
   * @param reason - Reason for removal (for logging)
   */
  evict(connectionId: string, reason = 'Manual eviction'): void {
    const conn = this.findConnectionById(connectionId);
    if (!conn) return;

    this.removeConnection(conn, reason);
  }

  /**
   * Removes all connections for a specific DO
   *
   * @param namespace - DO namespace
   * @param doId - DO identifier
   * @param reason - Reason for removal
   * @returns Number of connections removed
   */
  evictAll(namespace: string, doId: string, reason = 'Bulk eviction'): number {
    const key = this.getKey(namespace, doId);
    const doConnections = this.connections.get(key);
    if (!doConnections) return 0;

    const count = doConnections.size;
    for (const conn of doConnections.values()) {
      this.removeConnection(conn, reason);
    }
    return count;
  }

  /**
   * Gets a connection by ID (without acquiring it)
   *
   * @param connectionId - Connection ID
   * @returns Connection info or undefined
   */
  getConnection(connectionId: string): PooledConnection<T> | undefined {
    const conn = this.findConnectionById(connectionId);
    return conn ? this.toPublicConnection(conn) : undefined;
  }

  /**
   * Lists all connections in the pool
   *
   * @returns Array of all connections
   */
  listConnections(): PooledConnection<T>[] {
    const result: PooledConnection<T>[] = [];
    for (const doConns of this.connections.values()) {
      for (const conn of doConns.values()) {
        result.push(this.toPublicConnection(conn));
      }
    }
    return result;
  }

  /**
   * Lists connections for a specific DO
   *
   * @param namespace - DO namespace
   * @param doId - DO identifier
   * @returns Array of connections for the DO
   */
  listConnectionsForDO(namespace: string, doId: string): PooledConnection<T>[] {
    const key = this.getKey(namespace, doId);
    const doConns = this.connections.get(key);
    if (!doConns) return [];
    return Array.from(doConns.values()).map(c => this.toPublicConnection(c));
  }

  // ===========================================================================
  // Public API - Statistics
  // ===========================================================================

  /**
   * Gets pool statistics
   */
  getStats(): DOConnectionPoolStats {
    const connections = this.listConnections();
    const now = Date.now();

    const inUseConnections = connections.filter(c => c.inUse).length;
    const healthyConnections = connections.filter(c => c.health === 'healthy').length;
    const unhealthyConnections = connections.filter(c => c.health === 'unhealthy').length;

    // Calculate connections by namespace
    const connectionsByNamespace = new Map<string, number>();
    for (const conn of connections) {
      const count = connectionsByNamespace.get(conn.namespace) ?? 0;
      connectionsByNamespace.set(conn.namespace, count + 1);
    }

    // Calculate average age
    const totalAge = connections.reduce((sum, c) => sum + (now - c.createdAt), 0);
    const averageConnectionAge = connections.length > 0 ? totalAge / connections.length : 0;

    // Calculate hit rate
    const totalAcquisitions = this.stats.hits + this.stats.misses;
    const hitRate = totalAcquisitions > 0 ? this.stats.hits / totalAcquisitions : 0;

    return {
      totalConnections: connections.length,
      inUseConnections,
      idleConnections: connections.length - inUseConnections,
      healthyConnections,
      unhealthyConnections,
      connectionsByNamespace,
      totalRequests: this.stats.totalRequests,
      successfulRequests: this.stats.successfulRequests,
      failedRequests: this.stats.failedRequests,
      acquisitions: this.stats.acquisitions,
      releases: this.stats.releases,
      evictions: this.stats.evictions,
      connectionsCreated: this.stats.connectionsCreated,
      averageConnectionAge,
      hitRate,
    };
  }

  /**
   * Resets pool statistics
   */
  resetStats(): void {
    this.stats = {
      totalRequests: 0,
      successfulRequests: 0,
      failedRequests: 0,
      acquisitions: 0,
      releases: 0,
      evictions: 0,
      connectionsCreated: 0,
      hits: 0,
      misses: 0,
    };
  }

  // ===========================================================================
  // Public API - Events
  // ===========================================================================

  /**
   * Registers an event listener
   */
  on<K extends keyof DOConnectionPoolEvents>(
    event: K,
    listener: PoolEventListener<K>
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.add(listener as PoolEventListener<keyof DOConnectionPoolEvents>);
    }
    return this;
  }

  /**
   * Removes an event listener
   */
  off<K extends keyof DOConnectionPoolEvents>(
    event: K,
    listener: PoolEventListener<K>
  ): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      listeners.delete(listener as PoolEventListener<keyof DOConnectionPoolEvents>);
    }
    return this;
  }

  // ===========================================================================
  // Public API - Lifecycle
  // ===========================================================================

  /**
   * Checks if the pool is closed
   */
  isClosed(): boolean {
    return this.closed;
  }

  /**
   * Closes the pool and releases all connections
   */
  close(): void {
    this.closed = true;

    // Stop health check timer
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = undefined;
    }

    // Reject all waiters
    for (const [key, waiters] of this.waiters) {
      for (const waiter of waiters) {
        clearTimeout(waiter.timeoutId);
        waiter.reject(new Error('Connection pool closed'));
      }
    }
    this.waiters.clear();

    // Remove all connections
    for (const doConns of this.connections.values()) {
      for (const conn of doConns.values()) {
        if (conn.idleTimer) {
          clearTimeout(conn.idleTimer);
        }
      }
    }
    this.connections.clear();
  }

  // ===========================================================================
  // Private Methods - Connection Management
  // ===========================================================================

  private getKey(namespace: string, doId: string): string {
    return `${namespace}:${doId}`;
  }

  private createConnection(namespace: string, doId: string): InternalConnection<T> {
    const now = Date.now();
    const id = `conn-${++this.connectionCounter}-${now.toString(36)}`;

    const stub = this.stubFactory(namespace, doId);

    const conn: InternalConnection<T> = {
      id,
      doId,
      namespace,
      stub,
      createdAt: now,
      lastUsedAt: now,
      health: 'unknown',
      consecutiveFailures: 0,
      totalRequests: 0,
      successfulRequests: 0,
      failedRequests: 0,
      inUse: true, // Created in-use state
    };

    // Add to pool
    const key = this.getKey(namespace, doId);
    let doConns = this.connections.get(key);
    if (!doConns) {
      doConns = new Map();
      this.connections.set(key, doConns);
    }
    doConns.set(id, conn);

    this.stats.connectionsCreated++;
    this.emit('connection:created', { connectionId: id, doId, namespace });

    return conn;
  }

  private getIdleConnection(key: string, preferHealthy: boolean): InternalConnection<T> | undefined {
    const doConns = this.connections.get(key);
    if (!doConns) return undefined;

    // Find idle connections
    const idleConnections = Array.from(doConns.values()).filter(c => !c.inUse);
    if (idleConnections.length === 0) return undefined;

    if (preferHealthy) {
      // Prefer healthy connections
      const healthy = idleConnections.find(c => c.health === 'healthy');
      if (healthy) return healthy;

      // Then unknown health
      const unknown = idleConnections.find(c => c.health === 'unknown');
      if (unknown) return unknown;

      // Check if unhealthy connections can be retried
      const now = Date.now();
      const retriable = idleConnections.find(c =>
        c.health === 'unhealthy' &&
        c.lastFailureAt &&
        (now - c.lastFailureAt) >= this.config.unhealthyRetryMs
      );
      if (retriable) {
        // Reset health for retry
        retriable.health = 'unknown';
        retriable.consecutiveFailures = 0;
        return retriable;
      }

      return undefined;
    }

    // Return any idle connection
    return idleConnections[0];
  }

  private markInUse(conn: InternalConnection<T>): void {
    conn.inUse = true;
    conn.lastUsedAt = Date.now();

    // Clear idle timer
    if (conn.idleTimer) {
      clearTimeout(conn.idleTimer);
      conn.idleTimer = undefined;
    }
  }

  private resetIdleTimer(conn: InternalConnection<T>): void {
    if (conn.idleTimer) {
      clearTimeout(conn.idleTimer);
    }

    if (this.config.idleTimeoutMs > 0) {
      conn.idleTimer = setTimeout(() => {
        this.removeConnection(conn, 'Idle timeout');
      }, this.config.idleTimeoutMs);
    }
  }

  private removeConnection(conn: InternalConnection<T>, reason: string): void {
    // Clear idle timer
    if (conn.idleTimer) {
      clearTimeout(conn.idleTimer);
      conn.idleTimer = undefined;
    }

    // Remove from pool
    const key = this.getKey(conn.namespace, conn.doId);
    const doConns = this.connections.get(key);
    if (doConns) {
      doConns.delete(conn.id);
      if (doConns.size === 0) {
        this.connections.delete(key);
      }
    }

    this.stats.evictions++;
    this.emit('connection:evicted', { connectionId: conn.id, doId: conn.doId, reason });
  }

  private updateConnectionHealth(conn: InternalConnection<T>, newHealth: ConnectionHealth): void {
    const oldHealth = conn.health;
    if (oldHealth === newHealth) return;

    conn.health = newHealth;
    this.emit('connection:healthChange', {
      connectionId: conn.id,
      doId: conn.doId,
      oldHealth,
      newHealth,
    });
  }

  private findConnectionById(connectionId: string): InternalConnection<T> | undefined {
    for (const doConns of this.connections.values()) {
      const conn = doConns.get(connectionId);
      if (conn) return conn;
    }
    return undefined;
  }

  private getTotalConnectionCount(): number {
    let total = 0;
    for (const doConns of this.connections.values()) {
      total += doConns.size;
    }
    return total;
  }

  private toPublicConnection(internal: InternalConnection<T>): PooledConnection<T> {
    return {
      id: internal.id,
      doId: internal.doId,
      namespace: internal.namespace,
      stub: internal.stub,
      createdAt: internal.createdAt,
      lastUsedAt: internal.lastUsedAt,
      lastSuccessAt: internal.lastSuccessAt,
      lastFailureAt: internal.lastFailureAt,
      health: internal.health,
      consecutiveFailures: internal.consecutiveFailures,
      totalRequests: internal.totalRequests,
      successfulRequests: internal.successfulRequests,
      failedRequests: internal.failedRequests,
      inUse: internal.inUse,
    };
  }

  // ===========================================================================
  // Private Methods - Waiter Management
  // ===========================================================================

  private addWaiter(key: string, waiter: ConnectionWaiter<T>): void {
    let waiters = this.waiters.get(key);
    if (!waiters) {
      waiters = [];
      this.waiters.set(key, waiters);
    }
    waiters.push(waiter);
  }

  private removeWaiter(key: string, waiter: ConnectionWaiter<T>): void {
    const waiters = this.waiters.get(key);
    if (!waiters) return;

    const index = waiters.indexOf(waiter);
    if (index !== -1) {
      waiters.splice(index, 1);
    }

    if (waiters.length === 0) {
      this.waiters.delete(key);
    }
  }

  private serveWaiters(key: string): void {
    const waiters = this.waiters.get(key);
    if (!waiters || waiters.length === 0) return;

    const conn = this.getIdleConnection(key, true);
    if (!conn) return;

    const waiter = waiters.shift()!;
    if (waiters.length === 0) {
      this.waiters.delete(key);
    }

    this.markInUse(conn);
    this.stats.hits++;
    this.emit('connection:acquired', { connectionId: conn.id, doId: conn.doId });
    waiter.resolve(this.toPublicConnection(conn));
  }

  // ===========================================================================
  // Private Methods - Health Check
  // ===========================================================================

  private runHealthCheck(): void {
    const now = Date.now();

    for (const doConns of this.connections.values()) {
      for (const conn of doConns.values()) {
        // Skip in-use connections
        if (conn.inUse) continue;

        // Check for stale connections (no activity for a long time)
        const idleTime = now - conn.lastUsedAt;
        if (idleTime > this.config.idleTimeoutMs * 2) {
          this.removeConnection(conn, 'Health check: stale connection');
        }
      }
    }
  }

  // ===========================================================================
  // Private Methods - Events
  // ===========================================================================

  private emit<K extends keyof DOConnectionPoolEvents>(event: K, data: DOConnectionPoolEvents[K]): void {
    const listeners = this.eventListeners.get(event);
    if (!listeners) return;

    for (const listener of listeners) {
      try {
        (listener as PoolEventListener<K>)(data);
      } catch (error) {
        // Don't let listener errors break the pool
        console.error(`Error in connection pool ${event} listener:`, error);
      }
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Creates a new DO connection pool
 *
 * @param stubFactory - Factory function to create DO stubs
 * @param config - Pool configuration
 * @returns A new DOConnectionPool instance
 *
 * @example
 * ```typescript
 * const pool = createDOConnectionPool(
 *   (namespace, doId) => {
 *     const ns = env[namespace] as DurableObjectNamespace;
 *     return ns.get(ns.idFromName(doId));
 *   },
 *   {
 *     maxConnectionsPerDO: 5,
 *     idleTimeoutMs: 30000,
 *   }
 * );
 * ```
 */
export function createDOConnectionPool<T = unknown>(
  stubFactory: DOStubFactory<T>,
  config?: Partial<DOConnectionPoolConfig>
): DOConnectionPool<T> {
  return new DOConnectionPool(stubFactory, config);
}

// =============================================================================
// Pooled ShardRPC Implementation
// =============================================================================

/**
 * ShardRPC implementation that uses connection pooling
 *
 * This provides an implementation of the ShardRPC interface (from sharding module)
 * that uses a connection pool for efficient DO stub reuse.
 *
 * @example
 * ```typescript
 * const pooledRpc = createPooledShardRPC(
 *   (namespace, doId) => {
 *     const ns = env[namespace] as DurableObjectNamespace;
 *     return ns.get(ns.idFromName(doId));
 *   },
 *   { maxConnectionsPerDO: 5 }
 * );
 *
 * // Use with sharding executor
 * const executor = createExecutor(pooledRpc, replicaSelector, config);
 * ```
 */
export interface PooledShardRPCConfig extends Partial<DOConnectionPoolConfig> {
  /** Default timeout for RPC requests (ms) */
  defaultTimeoutMs?: number;
  /** DO namespace to use for shards (default: 'SHARD_DO') */
  defaultNamespace?: string;
}

/**
 * Stub interface for Durable Objects with fetch method
 */
export interface DOStub {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
}

/**
 * Result from a shard query execution
 */
export interface ShardResult {
  shardId: string;
  columns: string[];
  rows: unknown[][];
  rowCount: number;
  executionTimeMs: number;
  error?: {
    code: string;
    message: string;
    isRetryable: boolean;
  };
}

/**
 * Options for shard query execution
 */
export interface ShardExecuteOptions {
  timeoutMs?: number;
  maxRows?: number;
  includeColumnTypes?: boolean;
}

/**
 * ShardRPC interface for cross-DO communication
 */
export interface ShardRPC {
  execute(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ShardExecuteOptions
  ): Promise<ShardResult>;

  executeStream(
    shardId: string,
    replicaId: string | undefined,
    sql: string,
    params?: unknown[],
    options?: ShardExecuteOptions
  ): AsyncIterable<ShardResult>;
}

/**
 * Creates a pooled ShardRPC implementation
 *
 * @param stubFactory - Factory function to create DO stubs
 * @param config - Pool and RPC configuration
 * @returns ShardRPC implementation with connection pooling
 */
export function createPooledShardRPC(
  stubFactory: DOStubFactory<DOStub>,
  config: PooledShardRPCConfig = {}
): ShardRPC & { pool: DOConnectionPool<DOStub> } {
  const {
    defaultTimeoutMs = 30000,
    defaultNamespace = 'SHARD_DO',
    ...poolConfig
  } = config;

  const pool = createDOConnectionPool(stubFactory, poolConfig);

  return {
    pool,

    async execute(
      shardId: string,
      replicaId: string | undefined,
      sql: string,
      params?: unknown[],
      options?: ShardExecuteOptions
    ): Promise<ShardResult> {
      const targetId = replicaId ?? shardId;
      const startTime = performance.now();

      return pool.withConnection(defaultNamespace, targetId, async (stub) => {
        const timeoutMs = options?.timeoutMs ?? defaultTimeoutMs;

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

        try {
          const response = await stub.fetch('/rpc/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, params, options }),
            signal: controller.signal,
          });

          if (!response.ok) {
            const error = await response.text();
            return {
              shardId,
              columns: [],
              rows: [],
              rowCount: 0,
              executionTimeMs: performance.now() - startTime,
              error: {
                code: 'RPC_ERROR',
                message: error,
                isRetryable: response.status >= 500,
              },
            };
          }

          const result = await response.json() as { columns: string[]; rows: unknown[][]; rowCount: number };
          return {
            shardId,
            columns: result.columns,
            rows: result.rows,
            rowCount: result.rowCount,
            executionTimeMs: performance.now() - startTime,
          };
        } finally {
          clearTimeout(timeoutId);
        }
      });
    },

    async *executeStream(
      shardId: string,
      replicaId: string | undefined,
      sql: string,
      params?: unknown[],
      options?: ShardExecuteOptions
    ): AsyncIterable<ShardResult> {
      // For now, just wrap execute - true streaming would require WebSocket
      yield await this.execute(shardId, replicaId, sql, params, options);
    },
  };
}
