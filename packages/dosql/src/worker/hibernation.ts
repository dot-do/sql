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
import { createLogger } from '../logging/index.js';

const logger = createLogger({ defaultContext: { module: 'hibernation' } });

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
  /** Custom idle timeout for this connection (ms) */
  idleTimeout?: number;
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

/**
 * Alarm cleanup configuration.
 */
export interface AlarmCleanupConfig {
  /** Default idle timeout for connections (ms) */
  defaultIdleTimeout: number;
  /** Default transaction timeout (ms) */
  defaultTransactionTimeout: number;
  /** Minimum time between alarm checks (ms) */
  minAlarmInterval: number;
}

/**
 * Cloudflare hibernatable WebSocket with attachment API
 */
interface HibernatableWebSocket extends WebSocket {
  serializeAttachment(data: unknown): void;
  deserializeAttachment(): unknown;
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
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- TypeScript mixin pattern requires any[] for constructor parameters
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

    /** @internal Alarm cleanup configuration */
    protected alarmConfig: AlarmCleanupConfig = {
      defaultIdleTimeout: 30000,
      defaultTransactionTimeout: 30000,
      minAlarmInterval: 1000,
    };

    /** @internal Next scheduled alarm time */
    private nextAlarmTime: number | null = null;

    /**
     * Gets the DurableObjectState for WebSocket management.
     * Override in subclass if state is stored differently.
     */
    protected getState(): DurableObjectState {
      return (this as unknown as { ctx: DurableObjectState }).ctx;
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
      (ws as HibernatableWebSocket).serializeAttachment(sessionState);

      // Schedule cleanup alarm for this connection (async, don't await)
      this.scheduleCleanupForConnection().catch(e => {
        logger.error('Failed to schedule cleanup for connection', e instanceof Error ? e : new Error(String(e)));
      });
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
      return (ws as HibernatableWebSocket).deserializeAttachment() as WebSocketSessionState | undefined;
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
        (ws as HibernatableWebSocket).serializeAttachment(newState);
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
      logger.debug('WebSocket message received', { type: typeof message === 'string' ? 'text' : 'binary', preview: typeof message === 'string' ? message.slice(0, 100) : `${message.byteLength} bytes` });
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
        logger.info('WebSocket closed', { sessionId: session.sessionId, code, reason, clean: wasClean });

        // Clean up any active transaction
        if (session.transaction) {
          logger.info('Cleaning up transaction', { txId: session.transaction.txId });
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
        logger.error('WebSocket error', error instanceof Error ? error : new Error(String(error)), { sessionId: session.sessionId });

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
          logger.error('Broadcast error', e instanceof Error ? e : new Error(String(e)));
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

    // =========================================================================
    // Alarm-based Cleanup
    // =========================================================================

    /**
     * Schedules a cleanup alarm for the given time.
     * Uses alarm coalescing to avoid multiple wakes - only schedules
     * if this alarm is earlier than any existing alarm.
     *
     * @param cleanupTime - When to run cleanup (ms since epoch)
     */
    protected async scheduleCleanupAlarm(cleanupTime: number): Promise<void> {
      const state = this.getState();
      const now = Date.now();

      // Don't schedule alarms in the past
      if (cleanupTime <= now) {
        cleanupTime = now + this.alarmConfig.minAlarmInterval;
      }

      // Check if we already have an earlier alarm scheduled
      if (this.nextAlarmTime !== null && this.nextAlarmTime <= cleanupTime) {
        return; // Existing alarm is earlier, no need to reschedule
      }

      // Schedule the alarm
      try {
        await state.storage.setAlarm(cleanupTime);
        this.nextAlarmTime = cleanupTime;
        logger.debug('Scheduled cleanup alarm', { cleanupTime: new Date(cleanupTime).toISOString() });
      } catch (e) {
        logger.error('Failed to schedule cleanup alarm', e instanceof Error ? e : new Error(String(e)));
      }
    }

    /**
     * Calculates the next cleanup time based on all active connections.
     * Returns the earliest of:
     * - Idle connection timeouts
     * - Transaction timeouts
     *
     * @returns The next cleanup time (ms since epoch) or null if no cleanup needed
     */
    protected calculateNextCleanupTime(): number | null {
      const sockets = this.getWebSockets();
      if (sockets.length === 0) {
        return null;
      }

      let earliestCleanup: number | null = null;
      const now = Date.now();

      for (const ws of sockets) {
        const session = this.getSessionState(ws);
        if (!session) continue;

        // Check idle timeout
        const idleTimeout = this.getIdleTimeoutForSocket(ws);
        if (idleTimeout > 0) {
          const idleExpiry = session.lastActivity + idleTimeout;
          if (earliestCleanup === null || idleExpiry < earliestCleanup) {
            earliestCleanup = idleExpiry;
          }
        }

        // Check transaction timeout
        if (session.transaction) {
          const txExpiry = session.transaction.startedAt + session.transaction.timeout;
          if (earliestCleanup === null || txExpiry < earliestCleanup) {
            earliestCleanup = txExpiry;
          }
        }
      }

      return earliestCleanup;
    }

    /**
     * Gets the idle timeout for a socket from its session state.
     *
     * @param ws - The WebSocket to check
     * @returns The idle timeout in ms, or the default
     */
    private getIdleTimeoutForSocket(ws: WebSocket): number {
      const session = this.getSessionState(ws);
      if (session && 'idleTimeout' in session && typeof session.idleTimeout === 'number') {
        return session.idleTimeout;
      }
      return this.alarmConfig.defaultIdleTimeout;
    }

    /**
     * Handles alarm wake-up for cleanup tasks.
     * Override this in subclass for custom cleanup logic.
     *
     * Default behavior:
     * - Closes idle connections
     * - Aborts expired transactions
     * - Reschedules next cleanup alarm
     */
    async alarm(): Promise<void> {
      this.wakeReason = 'alarm';
      this.recordWake();
      this.nextAlarmTime = null;

      logger.debug('Alarm triggered for cleanup');

      const now = Date.now();
      const sockets = this.getWebSockets();

      for (const ws of sockets) {
        const session = this.getSessionState(ws);
        if (!session) continue;

        // Check for expired transaction
        if (session.transaction) {
          const txExpiry = session.transaction.startedAt + session.transaction.timeout;
          if (now >= txExpiry) {
            logger.info('Aborting expired transaction', {
              txId: session.transaction.txId,
              sessionId: session.sessionId,
            });
            await this.handleTransactionTimeout(ws, session);
          }
        }

        // Check for idle connection
        const idleTimeout = this.getIdleTimeoutForSocket(ws);
        if (idleTimeout > 0) {
          const idleExpiry = session.lastActivity + idleTimeout;
          if (now >= idleExpiry) {
            logger.info('Closing idle connection', {
              sessionId: session.sessionId,
              idleDuration: now - session.lastActivity,
            });
            await this.handleIdleTimeout(ws, session);
          }
        }
      }

      // Schedule next cleanup alarm
      const nextCleanup = this.calculateNextCleanupTime();
      if (nextCleanup !== null) {
        await this.scheduleCleanupAlarm(nextCleanup);
      }

      this.scheduleHibernation();
    }

    /**
     * Handles transaction timeout. Override in subclass to rollback transactions.
     *
     * @param ws - The WebSocket with the expired transaction
     * @param session - The session state
     */
    protected async handleTransactionTimeout(
      ws: WebSocket,
      session: WebSocketSessionState
    ): Promise<void> {
      // Clear transaction state by getting current state and removing transaction
      const current = this.getSessionState(ws);
      if (current) {
        const { transaction: _, ...rest } = current;
        (ws as HibernatableWebSocket).serializeAttachment({ ...rest, lastActivity: Date.now() });
      }

      // Notify client of timeout
      this.sendResponse(ws, {
        id: 'system',
        error: {
          code: -32000,
          message: `Transaction ${session.transaction?.txId} timed out`,
        },
      });
    }

    /**
     * Handles idle connection timeout. Override in subclass for custom behavior.
     *
     * @param ws - The idle WebSocket
     * @param session - The session state
     */
    protected async handleIdleTimeout(
      ws: WebSocket,
      session: WebSocketSessionState
    ): Promise<void> {
      // Close the connection
      try {
        ws.close(1000, 'Idle timeout');
      } catch {
        // Connection may already be closed
      }
    }

    /**
     * Called after accepting a WebSocket to schedule cleanup if needed.
     * This ensures alarms are set for new connections.
     */
    protected async scheduleCleanupForConnection(): Promise<void> {
      const nextCleanup = this.calculateNextCleanupTime();
      if (nextCleanup !== null) {
        await this.scheduleCleanupAlarm(nextCleanup);
      }
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
    return (this as unknown as { ctx: DurableObjectState }).ctx;
  }
}
