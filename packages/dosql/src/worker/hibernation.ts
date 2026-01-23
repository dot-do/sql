/**
 * Hibernation-aware WebSocket Handling for Durable Objects
 *
 * This module provides hibernation support for DoSQL Durable Objects.
 * Using Cloudflare's hibernation API, the DO can sleep while keeping
 * WebSocket connections open, reducing costs by ~95% for idle connections.
 *
 * Key concepts:
 * - acceptWebSocket: Tells the runtime to manage the WebSocket for hibernation
 * - webSocketMessage: Called when DO wakes to handle a message
 * - webSocketClose: Called when DO wakes to handle connection close
 * - webSocketError: Called when DO wakes to handle connection errors
 * - WebSocket attachments: State persisted across hibernation cycles
 *
 * @packageDocumentation
 */

import { DurableObject } from 'cloudflare:workers';

// =============================================================================
// Types
// =============================================================================

/**
 * Session state attached to WebSocket for persistence across hibernation.
 * This state survives DO sleep/wake cycles.
 */
export interface WebSocketSessionState {
  /** Unique session identifier */
  sessionId: string;
  /** Client identifier (for monitoring/debugging) */
  clientId?: string;
  /** Database being accessed */
  database?: string;
  /** Branch being accessed */
  branch?: string;
  /** When the connection was established */
  connectedAt: number;
  /** Last activity timestamp */
  lastActivity: number;
  /** Pending RPC request IDs */
  pendingRequests: string[];
  /** Active transaction state if any */
  transaction?: {
    txId: string;
    startedAt: number;
    timeout: number;
  };
  /** Prepared statement cache (serialized) */
  preparedStatements?: [string, string][];
  /** Metrics for this connection */
  metrics: {
    totalQueries: number;
    totalErrors: number;
    bytesReceived: number;
    bytesSent: number;
  };
}

/**
 * Tags that can be attached to WebSocket connections for management.
 */
export type WebSocketTag =
  | `client:${string}`
  | `database:${string}`
  | `branch:${string}`
  | `tx:${string}`
  | `session:${string}`
  | `notify:${string}`;

/**
 * RPC message format for WebSocket communication.
 */
export interface RPCMessage {
  id: string;
  method: string;
  params: unknown;
}

/**
 * RPC response format.
 */
export interface RPCResponse {
  id: string;
  result?: unknown;
  error?: {
    code: number;
    message: string;
    details?: unknown;
  };
}

/**
 * Hibernation statistics for monitoring.
 */
export interface HibernationStats {
  /** Total number of hibernation cycles */
  totalSleeps: number;
  /** Total number of wake-ups */
  totalWakes: number;
  /** Average sleep duration (ms) */
  averageSleepDuration: number;
  /** Total time spent sleeping (ms) */
  totalSleepTime: number;
  /** Estimated CPU time saved (ms) */
  cpuTimeSaved: number;
}

// =============================================================================
// Hibernation Mixin
// =============================================================================

/**
 * Mixin to add hibernation support to any Durable Object class.
 *
 * Usage:
 * ```typescript
 * export class MyDatabase extends HibernationMixin(BaseDurableObject) {
 *   // Your DO implementation
 * }
 * ```
 *
 * @param Base - The base Durable Object class to extend
 * @returns A class with hibernation support
 */
export function HibernationMixin<T extends new (...args: any[]) => DurableObject>(Base: T) {
  return class extends Base {
    /** @internal Hibernation statistics */
    private hibernationStats: HibernationStats = {
      totalSleeps: 0,
      totalWakes: 0,
      averageSleepDuration: 0,
      totalSleepTime: 0,
      cpuTimeSaved: 0,
    };

    /** @internal Last sleep start time */
    private lastSleepStart: number | null = null;

    /** @internal Track wake reasons */
    private wakeReason: 'message' | 'close' | 'error' | 'alarm' | 'fetch' | null = null;

    /**
     * Gets the DurableObjectState for WebSocket management.
     * Override in subclass if state is stored differently.
     */
    protected getState(): DurableObjectState {
      return (this as any).ctx;
    }

    /**
     * Accepts a WebSocket connection for hibernation management.
     *
     * Call this instead of handling the WebSocket directly to enable hibernation.
     * The DO can sleep while the connection remains open.
     *
     * @param ws - The WebSocket to accept
     * @param tags - Optional tags for connection management
     * @param initialState - Initial session state to attach
     */
    protected acceptWebSocket(
      ws: WebSocket,
      tags?: WebSocketTag[],
      initialState?: Partial<WebSocketSessionState>
    ): void {
      const state = this.getState();

      // Create session state
      const sessionState: WebSocketSessionState = {
        sessionId: crypto.randomUUID(),
        connectedAt: Date.now(),
        lastActivity: Date.now(),
        pendingRequests: [],
        metrics: {
          totalQueries: 0,
          totalErrors: 0,
          bytesReceived: 0,
          bytesSent: 0,
        },
        ...initialState,
      };

      // Add session tag
      const allTags = [...(tags ?? []), `session:${sessionState.sessionId}` as WebSocketTag];

      // Accept for hibernation management
      state.acceptWebSocket(ws, allTags);

      // Attach session state (survives hibernation)
      (ws as any).serializeAttachment(sessionState);
    }

    /**
     * Gets all managed WebSocket connections, optionally filtered by tag.
     *
     * @param tag - Optional tag to filter connections
     * @returns Array of WebSocket connections
     */
    protected getWebSockets(tag?: WebSocketTag): WebSocket[] {
      const state = this.getState();
      return state.getWebSockets(tag);
    }

    /**
     * Gets the session state attached to a WebSocket.
     *
     * @param ws - The WebSocket to get state from
     * @returns The session state or undefined if not found
     */
    protected getSessionState(ws: WebSocket): WebSocketSessionState | undefined {
      return (ws as any).deserializeAttachment() as WebSocketSessionState | undefined;
    }

    /**
     * Updates the session state attached to a WebSocket.
     *
     * @param ws - The WebSocket to update
     * @param updates - Partial state updates
     */
    protected updateSessionState(
      ws: WebSocket,
      updates: Partial<WebSocketSessionState>
    ): void {
      const current = this.getSessionState(ws);
      if (current) {
        const newState = { ...current, ...updates, lastActivity: Date.now() };
        (ws as any).serializeAttachment(newState);
      }
    }

    /**
     * Handles incoming WebSocket messages during hibernation wake.
     *
     * Override this method to implement your message handling logic.
     * The default implementation just logs the message.
     *
     * @param ws - The WebSocket that received the message
     * @param message - The message data
     */
    async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
      this.wakeReason = 'message';
      this.recordWake();

      const session = this.getSessionState(ws);
      if (session) {
        // Update metrics
        const bytes = typeof message === 'string' ? message.length : message.byteLength;
        this.updateSessionState(ws, {
          metrics: {
            ...session.metrics,
            bytesReceived: session.metrics.bytesReceived + bytes,
          },
        });
      }

      // Default: echo back (override in subclass for real handling)
      console.log('[Hibernation] WebSocket message received:', typeof message === 'string' ? message.slice(0, 100) : `[binary ${message.byteLength} bytes]`);
    }

    /**
     * Handles WebSocket close during hibernation wake.
     *
     * Override this method to implement your cleanup logic.
     *
     * @param ws - The WebSocket that closed
     * @param code - The close code
     * @param reason - The close reason
     * @param wasClean - Whether it was a clean close
     */
    async webSocketClose(
      ws: WebSocket,
      code: number,
      reason: string,
      wasClean: boolean
    ): Promise<void> {
      this.wakeReason = 'close';
      this.recordWake();

      const session = this.getSessionState(ws);
      if (session) {
        console.log(`[Hibernation] WebSocket closed: session=${session.sessionId}, code=${code}, reason=${reason}, clean=${wasClean}`);

        // Clean up any active transaction
        if (session.transaction) {
          console.log(`[Hibernation] Cleaning up transaction: ${session.transaction.txId}`);
          // Override in subclass to actually rollback the transaction
        }
      }
    }

    /**
     * Handles WebSocket errors during hibernation wake.
     *
     * Override this method to implement your error handling logic.
     *
     * @param ws - The WebSocket that errored
     * @param error - The error that occurred
     */
    async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
      this.wakeReason = 'error';
      this.recordWake();

      const session = this.getSessionState(ws);
      if (session) {
        console.error(`[Hibernation] WebSocket error: session=${session.sessionId}`, error);

        // Update error metrics
        this.updateSessionState(ws, {
          metrics: {
            ...session.metrics,
            totalErrors: session.metrics.totalErrors + 1,
          },
        });
      }
    }

    /**
     * Records a wake event and updates statistics.
     * @internal
     */
    private recordWake(): void {
      this.hibernationStats.totalWakes++;

      if (this.lastSleepStart !== null) {
        const sleepDuration = Date.now() - this.lastSleepStart;
        this.hibernationStats.totalSleepTime += sleepDuration;
        this.hibernationStats.averageSleepDuration =
          this.hibernationStats.totalSleepTime / this.hibernationStats.totalWakes;

        // Estimate CPU time saved (assume ~1% CPU usage during sleep vs active)
        this.hibernationStats.cpuTimeSaved += sleepDuration * 0.99;

        this.lastSleepStart = null;
      }
    }

    /**
     * Gets hibernation statistics.
     *
     * @returns Current hibernation statistics
     */
    getHibernationStats(): HibernationStats {
      return { ...this.hibernationStats };
    }

    /**
     * Broadcasts a message to all WebSocket connections, optionally filtered by tag.
     *
     * @param message - The message to send
     * @param tag - Optional tag to filter recipients
     */
    protected broadcast(message: string | ArrayBuffer, tag?: WebSocketTag): void {
      const sockets = this.getWebSockets(tag);
      for (const ws of sockets) {
        try {
          ws.send(message);

          // Update metrics
          const session = this.getSessionState(ws);
          if (session) {
            const bytes = typeof message === 'string' ? message.length : message.byteLength;
            this.updateSessionState(ws, {
              metrics: {
                ...session.metrics,
                bytesSent: session.metrics.bytesSent + bytes,
              },
            });
          }
        } catch (e) {
          // Connection may have closed
          console.error('[Hibernation] Broadcast error:', e);
        }
      }
    }

    /**
     * Sends an RPC response to a WebSocket.
     *
     * @param ws - The WebSocket to respond on
     * @param response - The RPC response
     */
    protected sendResponse(ws: WebSocket, response: RPCResponse): void {
      const message = JSON.stringify(response);
      ws.send(message);

      // Update metrics
      const session = this.getSessionState(ws);
      if (session) {
        // Remove from pending if completed
        const pendingRequests = session.pendingRequests.filter(id => id !== response.id);
        this.updateSessionState(ws, {
          pendingRequests,
          metrics: {
            ...session.metrics,
            bytesSent: session.metrics.bytesSent + message.length,
          },
        });
      }
    }

    /**
     * Handles WebSocket upgrade in fetch handler.
     *
     * Call this from your fetch handler to accept WebSocket connections.
     *
     * @param request - The incoming request
     * @param tags - Optional tags for the connection
     * @returns Response with WebSocket upgrade
     */
    protected handleWebSocketUpgrade(
      request: Request,
      tags?: WebSocketTag[]
    ): Response {
      const upgradeHeader = request.headers.get('Upgrade');
      if (upgradeHeader !== 'websocket') {
        return new Response('Expected WebSocket upgrade', { status: 400 });
      }

      const [client, server] = Object.values(new WebSocketPair());

      // Extract client info from headers for tags
      const clientId = request.headers.get('X-Client-ID');
      const database = new URL(request.url).searchParams.get('database') ?? undefined;
      const branch = new URL(request.url).searchParams.get('branch') ?? undefined;

      const connectionTags: WebSocketTag[] = [...(tags ?? [])];
      if (clientId) connectionTags.push(`client:${clientId}` as WebSocketTag);
      if (database) connectionTags.push(`database:${database}` as WebSocketTag);
      if (branch) connectionTags.push(`branch:${branch}` as WebSocketTag);

      // Accept for hibernation
      this.acceptWebSocket(server, connectionTags, { clientId, database, branch });

      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    }

    /**
     * Schedules the next hibernation opportunity.
     *
     * Call this after completing message handling to allow hibernation.
     * @internal
     */
    protected scheduleHibernation(): void {
      this.lastSleepStart = Date.now();
      this.hibernationStats.totalSleeps++;
      // DO will hibernate automatically when this function returns
      // and there's no more work to do
    }
  };
}

// =============================================================================
// Helper to create a hibernation-aware DO class
// =============================================================================

/**
 * Base class for hibernation-aware Durable Objects.
 *
 * Extend this class to create a DO that supports WebSocket hibernation.
 *
 * @example
 * ```typescript
 * export class MyDatabase extends HibernatingDurableObject {
 *   async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
 *     const data = JSON.parse(message as string);
 *     // Handle RPC message
 *     this.sendResponse(ws, { id: data.id, result: 'ok' });
 *     this.scheduleHibernation();
 *   }
 * }
 * ```
 */
export class HibernatingDurableObject extends HibernationMixin(DurableObject) {
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
  }

  protected getState(): DurableObjectState {
    return (this as any).ctx;
  }
}
