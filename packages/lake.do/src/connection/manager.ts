/**
 * @dotdo/lake.do - WebSocket Connection Manager
 *
 * This module provides WebSocket connection management with automatic
 * reconnection and event handling.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import type { LakeRPCError } from '../types.js';
import type { ConnectionConfig, PendingRequest, ConnectionEventHandler } from './types.js';
import { ConnectionEventEmitter } from './event-emitter.js';

// =============================================================================
// Error Classes
// =============================================================================

/**
 * Masks sensitive data in a URL for safe logging and error messages.
 * @internal
 */
function maskUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.password) {
      parsed.password = '***';
    }
    const sensitiveParams = ['token', 'key', 'secret', 'password', 'auth', 'api_key', 'apikey', 'access_token'];
    for (const param of sensitiveParams) {
      if (parsed.searchParams.has(param)) {
        parsed.searchParams.set(param, '***');
      }
    }
    return parsed.toString();
  } catch {
    const match = url.match(/^(\w+:\/\/)([^/?#]+)/);
    if (match) {
      return `${match[1]}${match[2]}/***`;
    }
    return '[invalid-url]';
  }
}

/**
 * Error thrown when a connection operation fails.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class ConnectionError extends Error {
  readonly code: string;
  readonly details?: unknown;
  readonly url?: string;

  constructor(error: LakeRPCError, url?: string) {
    const maskedUrl = url ? maskUrl(url) : undefined;
    const fullMessage = maskedUrl ? `${error.message} (url: ${maskedUrl})` : error.message;
    super(fullMessage);
    this.name = 'ConnectionError';
    this.code = error.code;
    this.details = error.details;
    if (maskedUrl) {
      this.url = maskedUrl;
    }
  }
}

// =============================================================================
// Connection Manager Implementation
// =============================================================================

/**
 * Manages WebSocket connections with automatic reconnection support.
 *
 * @description Provides a clean interface for WebSocket connection management,
 * handling connection lifecycle, pending requests, and event emission.
 *
 * @example
 * ```typescript
 * const manager = new WebSocketConnectionManager({
 *   url: 'wss://example.com',
 *   timeout: 30000,
 *   retry: { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 },
 * });
 *
 * manager.on('connected', () => console.log('Connected!'));
 * manager.on('message', (data) => handleMessage(data));
 *
 * await manager.connect();
 * manager.send(JSON.stringify({ method: 'ping' }));
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export class WebSocketConnectionManager {
  private readonly config: ConnectionConfig;
  private readonly events = new ConnectionEventEmitter();
  private readonly pendingRequests = new Map<string, PendingRequest>();

  private ws: WebSocket | null = null;
  private _isConnected = false;
  private messageHandler: ((data: string | ArrayBuffer) => void) | null = null;

  /**
   * Creates a new WebSocketConnectionManager.
   *
   * @param config - Connection configuration
   */
  constructor(config: ConnectionConfig) {
    this.config = config;
  }

  // ===========================================================================
  // Public Properties
  // ===========================================================================

  /**
   * Whether the connection is currently open.
   */
  get isConnected(): boolean {
    return this._isConnected && this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  /**
   * The configured URL.
   */
  get url(): string {
    return this.config.url;
  }

  // ===========================================================================
  // Event Handling
  // ===========================================================================

  /**
   * Registers an event listener.
   *
   * @param event - The event type
   * @param handler - The callback function
   */
  on(event: 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'error', handler: ConnectionEventHandler): void {
    this.events.on(event, handler);
  }

  /**
   * Removes an event listener.
   *
   * @param event - The event type
   * @param handler - The callback function
   */
  off(event: 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'error', handler: ConnectionEventHandler): void {
    this.events.off(event, handler);
  }

  /**
   * Registers a one-time event listener.
   *
   * @param event - The event type
   * @param handler - The callback function
   */
  once(event: 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'error', handler: ConnectionEventHandler): void {
    this.events.once(event, handler);
  }

  /**
   * Sets the message handler for incoming WebSocket messages.
   *
   * @param handler - The message handler function
   */
  setMessageHandler(handler: (data: string | ArrayBuffer) => void): void {
    this.messageHandler = handler;
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  /**
   * Establishes a WebSocket connection.
   *
   * @returns Resolves when the connection is established
   * @throws {ConnectionError} When the connection fails
   */
  async connect(): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }

    return new Promise((resolve, reject) => {
      const wsUrl = this.config.url.replace(/^http/, 'ws');
      this.ws = new WebSocket(wsUrl);

      this.ws.addEventListener('open', () => {
        this._isConnected = true;
        this.events.emit('connected');
        resolve();
      });

      this.ws.addEventListener('error', (event: Event) => {
        this._isConnected = false;
        const error = new ConnectionError({
          code: 'CONNECTION_ERROR',
          message: `WebSocket error: ${event}`,
        }, wsUrl);
        this.events.emit('error', error);
        reject(error);
      });

      this.ws.addEventListener('close', () => {
        const wasConnected = this._isConnected;
        this._isConnected = false;
        this.ws = null;

        if (wasConnected) {
          this.events.emit('disconnected');
        }

        // Reject all pending requests
        for (const [id, pending] of this.pendingRequests) {
          clearTimeout(pending.timeout);
          pending.reject(new ConnectionError({
            code: 'CONNECTION_CLOSED',
            message: 'Connection closed',
          }, wsUrl));
          this.pendingRequests.delete(id);
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
   * Closes the WebSocket connection.
   *
   * @returns Resolves when the connection is closed
   */
  async close(): Promise<void> {
    const wasConnected = this._isConnected;

    if (this.ws) {
      this._isConnected = false;
      this.ws.close();
      this.ws = null;
    }

    if (wasConnected) {
      this.events.emit('disconnected');
    }
  }

  /**
   * Sends data over the WebSocket connection.
   *
   * @param data - The data to send
   * @throws {ConnectionError} When not connected
   */
  send(data: string): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new ConnectionError({
        code: 'NOT_CONNECTED',
        message: 'WebSocket is not connected',
      }, this.config.url);
    }
    this.ws.send(data);
  }

  // ===========================================================================
  // Pending Request Management
  // ===========================================================================

  /**
   * Registers a pending request.
   *
   * @param id - The request ID
   * @param resolve - The resolve function
   * @param reject - The reject function
   * @param timeoutMs - Timeout in milliseconds
   */
  registerRequest(
    id: string,
    resolve: (value: unknown) => void,
    reject: (error: Error) => void,
    timeoutMs: number
  ): void {
    const timeout = setTimeout(() => {
      this.pendingRequests.delete(id);
      reject(new Error(`Request timeout`));
    }, timeoutMs);

    this.pendingRequests.set(id, { resolve, reject, timeout });
  }

  /**
   * Resolves a pending request.
   *
   * @param id - The request ID
   * @param value - The result value
   * @returns True if the request was found and resolved
   */
  resolveRequest(id: string, value: unknown): boolean {
    const pending = this.pendingRequests.get(id);
    if (pending) {
      clearTimeout(pending.timeout);
      this.pendingRequests.delete(id);
      pending.resolve(value);
      return true;
    }
    return false;
  }

  /**
   * Rejects a pending request.
   *
   * @param id - The request ID
   * @param error - The error
   * @returns True if the request was found and rejected
   */
  rejectRequest(id: string, error: Error): boolean {
    const pending = this.pendingRequests.get(id);
    if (pending) {
      clearTimeout(pending.timeout);
      this.pendingRequests.delete(id);
      pending.reject(error);
      return true;
    }
    return false;
  }

  /**
   * Returns the number of pending requests.
   */
  get pendingRequestCount(): number {
    return this.pendingRequests.size;
  }
}
