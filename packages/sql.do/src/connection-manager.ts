/**
 * WebSocket Connection Manager
 *
 * Manages WebSocket connections with support for connection pooling,
 * event handling, and connection lifecycle management.
 *
 * @packageDocumentation
 */

import type { PoolConfig, PoolStats, PoolHealth, ConnectionInfo, PoolEventMap } from './types.js';
import { ConnectionError } from './errors.js';
import { WebSocketPool, type PooledConnection } from './pool.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Connection event data for 'connected' event.
 * @public
 * @since 0.2.0
 */
export interface ConnectedEvent {
  url: string;
  timestamp: Date;
}

/**
 * Disconnection event data for 'disconnected' event.
 * @public
 * @since 0.2.0
 */
export interface DisconnectedEvent {
  url: string;
  timestamp: Date;
  reason?: string;
}

/**
 * Event types for connection manager.
 * @public
 */
export interface ConnectionEventMap {
  connected: ConnectedEvent;
  disconnected: DisconnectedEvent;
}

/**
 * Combined event map for both connection and pool events.
 * @public
 */
export type AllConnectionEventMap = ConnectionEventMap & PoolEventMap;

/**
 * Event listener callback type.
 * @public
 */
export type ConnectionEventListener<K extends keyof AllConnectionEventMap> = (event: AllConnectionEventMap[K]) => void;

/**
 * Configuration for the connection manager.
 * @internal
 */
export interface ConnectionManagerConfig {
  url: string;
  pool?: PoolConfig;
}

/**
 * Message handler callback type.
 * @internal
 */
export type MessageHandler = (data: string | ArrayBuffer) => void;

// =============================================================================
// Connection Manager
// =============================================================================

/**
 * Manages WebSocket connections with optional pooling support.
 *
 * Provides a unified interface for managing single connections or pooled connections,
 * with support for event handling and connection lifecycle management.
 *
 * @example
 * ```typescript
 * // Create a connection manager without pooling
 * const manager = new ConnectionManager({ url: 'wss://sql.example.com' });
 *
 * // With pooling
 * const pooledManager = new ConnectionManager({
 *   url: 'wss://sql.example.com',
 *   pool: { maxSize: 10, idleTimeout: 30000 },
 * });
 *
 * // Connect and send messages
 * const ws = await manager.getConnection();
 * ws.send(JSON.stringify({ method: 'ping' }));
 *
 * // Listen for events
 * manager.on('connected', (event) => console.log('Connected:', event.url));
 * manager.on('disconnected', (event) => console.log('Disconnected:', event.reason));
 *
 * // Clean up
 * await manager.close();
 * ```
 *
 * @public
 */
export class ConnectionManager {
  private readonly url: string;

  // Single connection state
  private ws: WebSocket | null = null;
  private connecting = false;
  private connectPromise: Promise<void> | null = null;

  // Connection pool (if configured)
  private pool: WebSocketPool | null = null;
  private pooledConnection: PooledConnection | null = null;

  // Event listeners - internal map uses generic function type; public on/off API is properly typed
  private eventListeners: Map<keyof AllConnectionEventMap, Set<(event: AllConnectionEventMap[keyof AllConnectionEventMap]) => void>> = new Map();

  // Message handler
  private messageHandler: MessageHandler | null = null;

  // Pending request tracking for cleanup on close
  private onConnectionClose: (() => void) | null = null;

  /**
   * Creates a new ConnectionManager.
   *
   * @param config - Connection configuration
   */
  constructor(config: ConnectionManagerConfig) {
    this.url = config.url.replace(/^http/, 'ws');

    // Initialize event listeners map
    this.eventListeners.set('connected', new Set());
    this.eventListeners.set('disconnected', new Set());

    // Initialize pool event listeners
    const poolEvents: (keyof PoolEventMap)[] = [
      'pool:connection-created',
      'pool:connection-reused',
      'pool:connection-closed',
      'pool:health-check',
      'pool:backpressure',
      'do:hibernating',
      'do:awake',
    ];
    poolEvents.forEach(event => this.eventListeners.set(event, new Set()));

    // Initialize connection pool if configured
    if (config.pool) {
      this.pool = new WebSocketPool(config.url, config.pool);

      // Forward pool events to manager event listeners
      poolEvents.forEach(event => {
        this.pool!.on(event, (data) => {
          this.emit(event, data);
        });
      });
    }
  }

  // ===========================================================================
  // Connection State
  // ===========================================================================

  /**
   * Checks if the manager is currently connected.
   *
   * @returns `true` if connected, `false` otherwise
   */
  isConnected(): boolean {
    // WebSocket ready states: 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED
    // READY_STATE_OPEN (1) means connection is established and ready to communicate
    if (this.pool && this.pooledConnection) {
      return this.pooledConnection.ws.readyState === WebSocket.READY_STATE_OPEN;
    }
    return this.ws !== null && this.ws.readyState === WebSocket.READY_STATE_OPEN;
  }

  /**
   * Gets the WebSocket URL being used.
   *
   * @returns The WebSocket URL
   */
  getUrl(): string {
    return this.url;
  }

  /**
   * Checks if the manager has a connection pool configured.
   *
   * @returns `true` if pool is configured
   */
  hasPool(): boolean {
    return this.pool !== null;
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  /**
   * Explicitly establishes a connection.
   *
   * If already connected, returns immediately. Multiple concurrent calls
   * will share the same connection attempt.
   *
   * @returns Promise that resolves when the connection is established
   * @throws {ConnectionError} When connection fails
   */
  async connect(): Promise<void> {
    if (this.isConnected()) {
      return;
    }

    // If already connecting, wait for that to complete
    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.connectPromise = this.performConnect();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  /**
   * Internal method to perform the actual connection.
   * @internal
   */
  private async performConnect(): Promise<void> {
    this.connecting = true;
    try {
      await this.getConnection();
    } finally {
      this.connecting = false;
    }
  }

  /**
   * Gets a WebSocket connection.
   *
   * Uses connection pool if configured, otherwise manages a single connection.
   *
   * @returns Promise resolving to the WebSocket connection
   * @throws {ConnectionError} When connection fails
   */
  async getConnection(): Promise<WebSocket> {
    if (this.pool) {
      return this.getPooledConnection();
    }
    return this.getSingleConnection();
  }

  /**
   * Sets the message handler for incoming messages.
   *
   * @param handler - Function to call when messages are received
   */
  setMessageHandler(handler: MessageHandler): void {
    this.messageHandler = handler;
  }

  /**
   * Sets the callback for connection close events.
   *
   * @param callback - Function to call when connection closes
   */
  setCloseCallback(callback: () => void): void {
    this.onConnectionClose = callback;
  }

  /**
   * Releases the current pooled connection back to the pool.
   *
   * Only applicable when using connection pooling.
   */
  releaseConnection(): void {
    if (this.pooledConnection && this.pool) {
      this.pool.release(this.pooledConnection);
      this.pooledConnection = null;
    }
  }

  /**
   * Closes all connections and releases resources.
   *
   * @returns Promise resolving when all connections are closed
   */
  async close(): Promise<void> {
    // Close pooled connection if using pool
    if (this.pool) {
      this.releaseConnection();
      await this.pool.close();
      this.pool = null;
    }

    // Close direct WebSocket connection
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  // ===========================================================================
  // Event Handling
  // ===========================================================================

  /**
   * Registers an event listener.
   *
   * @param event - The event name to listen for
   * @param listener - The callback function
   * @returns The manager instance (for chaining)
   */
  on<K extends keyof AllConnectionEventMap>(event: K, listener: ConnectionEventListener<K>): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      // Type assertion needed: listener is contravariant but TypeScript can't prove
      // that ConnectionEventListener<K> is compatible with the union type in the Set
      listeners.add(listener as (event: AllConnectionEventMap[keyof AllConnectionEventMap]) => void);
    }
    return this;
  }

  /**
   * Removes an event listener.
   *
   * @param event - The event name
   * @param listener - The callback function to remove
   * @returns The manager instance (for chaining)
   */
  off<K extends keyof AllConnectionEventMap>(event: K, listener: ConnectionEventListener<K>): this {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      // Type assertion needed: same reason as on() method
      listeners.delete(listener as (event: AllConnectionEventMap[keyof AllConnectionEventMap]) => void);
    }
    return this;
  }

  /**
   * Emits an event to all registered listeners.
   * @internal
   */
  private emit<K extends keyof AllConnectionEventMap>(event: K, data: AllConnectionEventMap[K]): void {
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

  // ===========================================================================
  // Pool Management
  // ===========================================================================

  /**
   * Gets statistics about the connection pool.
   *
   * @returns Pool statistics or undefined if pool is not configured
   */
  getPoolStats(): PoolStats | undefined {
    return this.pool?.getStats();
  }

  /**
   * Gets health status of the connection pool.
   *
   * @returns Pool health or undefined if pool is not configured
   */
  getPoolHealth(): PoolHealth | undefined {
    return this.pool?.getHealth();
  }

  /**
   * Gets information about all pooled connections.
   *
   * @returns Array of connection info or undefined if pool is not configured
   */
  getConnectionInfo(): ConnectionInfo[] | undefined {
    return this.pool?.getConnectionInfo();
  }

  /**
   * Gets the connection tags configured for the pool.
   *
   * @returns Array of tags or empty array if pool not configured
   */
  getConnectionTags(): string[] {
    return this.pool?.getConnectionTags() ?? [];
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  /**
   * Gets a single WebSocket connection (non-pooled).
   * @internal
   */
  private async getSingleConnection(): Promise<WebSocket> {
    // WebSocket.READY_STATE_OPEN = 1 means connection is established and ready to communicate
    if (this.ws && this.ws.readyState === WebSocket.READY_STATE_OPEN) {
      return this.ws;
    }

    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);

      this.ws.addEventListener('open', () => {
        this.emit('connected', {
          url: this.url,
          timestamp: new Date(),
        });
        resolve(this.ws!);
      });

      this.ws.addEventListener('error', (event: Event) => {
        reject(new ConnectionError(`WebSocket error: ${event}`, this.url));
      });

      this.ws.addEventListener('close', () => {
        const closedWs = this.ws;
        this.ws = null;

        this.emit('disconnected', {
          url: this.url,
          timestamp: new Date(),
          reason: 'Connection closed',
        });

        // Call close callback if set
        if (this.onConnectionClose) {
          this.onConnectionClose();
        }
      });

      this.ws.addEventListener('message', (event: MessageEvent) => {
        if (this.messageHandler) {
          this.messageHandler(event.data as string | ArrayBuffer);
        }
      });
    });
  }

  /**
   * Gets a connection from the pool.
   * @internal
   */
  private async getPooledConnection(): Promise<WebSocket> {
    // If we have a borrowed connection that's still open, use it
    // WebSocket.READY_STATE_OPEN = 1 means connection is established and ready to communicate
    if (this.pooledConnection && this.pooledConnection.ws.readyState === WebSocket.READY_STATE_OPEN) {
      return this.pooledConnection.ws;
    }

    // Release any stale connection
    if (this.pooledConnection) {
      this.pool!.release(this.pooledConnection);
      this.pooledConnection = null;
    }

    // Acquire a new connection from the pool
    this.pooledConnection = await this.pool!.acquire();

    // Set up message handler for this connection
    this.pooledConnection.ws.addEventListener('message', (event: MessageEvent) => {
      if (this.messageHandler) {
        this.messageHandler(event.data as string | ArrayBuffer);
      }
    });

    // Emit connected event
    this.emit('connected', {
      url: this.url,
      timestamp: new Date(),
    });

    return this.pooledConnection.ws;
  }
}
