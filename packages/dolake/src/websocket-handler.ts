/**
 * WebSocket Handler Module
 *
 * Handles WebSocket message handling, connection management, and hibernation support.
 * Extracted from the DoLake monolith for better separation of concerns.
 */

import {
  type CDCEvent,
  type CDCBatchMessage,
  type ConnectMessage,
  type HeartbeatMessage,
  type FlushRequestMessage,
  type AckMessage,
  type NackMessage,
  type StatusMessage,
  type WebSocketAttachment,
  type DoLakeState,
  type RateLimitInfo,
  isCDCBatchMessage,
  isConnectMessage,
  isHeartbeatMessage,
  isFlushRequestMessage,
  encodeCapabilities,
  generateUUID,
  BufferOverflowError,
} from './types.js';
import {
  RateLimiter,
  type RateLimitConfig,
  type RateLimitResult,
  DEFAULT_RATE_LIMIT_CONFIG,
} from './rate-limiter.js';
import {
  ClientRpcMessageSchema,
  MessageValidationError,
  type ValidatedClientRpcMessage,
} from './schemas.js';
import { CDCBufferManager, type BufferSnapshot } from './buffer.js';
import { serialize, bigintReviver } from './serialization.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Extended WebSocket attachment with IP tracking
 */
export interface ExtendedWebSocketAttachment extends WebSocketAttachment {
  /** Client IP address */
  clientIp?: string | undefined;
  /** Connection ID for rate limiting */
  connectionId: string;
}

/**
 * Interface for flush trigger callback
 */
export type FlushTriggerCallback = (trigger: string) => Promise<void>;

/**
 * Interface for flush result callback
 */
export type FlushCallback = (trigger: string) => Promise<{ success: boolean; [key: string]: unknown }>;

/**
 * WebSocket handler configuration
 */
export interface WebSocketHandlerConfig {
  rateLimitConfig: RateLimitConfig;
  circuitBreakerState: 'closed' | 'open' | 'half-open';
}

/**
 * Dependencies injected into the WebSocket handler
 */
export interface WebSocketHandlerDeps {
  ctx: DurableObjectState;
  buffer: CDCBufferManager;
  rateLimiter: RateLimiter;
  getState: () => DoLakeState;
  setState: (state: DoLakeState) => void;
  getCircuitBreakerState: () => 'closed' | 'open' | 'half-open';
  scheduleFlush: FlushTriggerCallback;
  flush: FlushCallback;
}

// =============================================================================
// WebSocket Handler Class
// =============================================================================

/**
 * Handles all WebSocket-related operations for DoLake
 *
 * @example
 * ```typescript
 * // Create a WebSocketHandler instance
 * const handler = new WebSocketHandler({
 *   ctx: durableObjectState,
 *   buffer: cdcBufferManager,
 *   rateLimiter: rateLimiter,
 *   getState: () => currentState,
 *   setState: (state) => { currentState = state; },
 *   getCircuitBreakerState: () => circuitBreaker.state,
 *   scheduleFlush: async (trigger) => { await scheduleFlushAlarm(trigger); },
 *   flush: async (trigger) => { return await performFlush(trigger); },
 * });
 * ```
 */
export class WebSocketHandler {
  private readonly deps: WebSocketHandlerDeps;
  private readonly rateLimitConfig: RateLimitConfig;

  /**
   * Create a new WebSocketHandler instance
   *
   * @param deps - Dependencies injected into the handler
   * @param config - Optional configuration overrides
   *
   * @example
   * ```typescript
   * const handler = new WebSocketHandler(
   *   {
   *     ctx: durableObjectState,
   *     buffer: new CDCBufferManager(),
   *     rateLimiter: new RateLimiter(),
   *     getState: () => this.state,
   *     setState: (s) => { this.state = s; },
   *     getCircuitBreakerState: () => 'closed',
   *     scheduleFlush: async (trigger) => { /* schedule */ },
   *     flush: async (trigger) => ({ success: true }),
   *   },
   *   { rateLimitConfig: { maxMessagesPerSecond: 100, maxPayloadSize: 1024 * 1024 } }
   * );
   * ```
   */
  constructor(deps: WebSocketHandlerDeps, config?: Partial<WebSocketHandlerConfig>) {
    this.deps = deps;
    this.rateLimitConfig = config?.rateLimitConfig ?? DEFAULT_RATE_LIMIT_CONFIG;
  }

  // ===========================================================================
  // WebSocket Upgrade
  // ===========================================================================

  /**
   * Handle WebSocket upgrade request.
   * Validates rate limits, creates the WebSocket pair, and registers the connection.
   *
   * @param request - The incoming HTTP request with WebSocket upgrade headers
   * @returns A Response with status 101 for successful upgrade, or 429 if rate limited
   *
   * @example
   * ```typescript
   * // In a Durable Object fetch handler
   * async fetch(request: Request): Promise<Response> {
   *   const url = new URL(request.url);
   *
   *   if (url.pathname === '/ws' && request.headers.get('Upgrade') === 'websocket') {
   *     return this.wsHandler.handleWebSocketUpgrade(request);
   *   }
   *
   *   return new Response('Not found', { status: 404 });
   * }
   * ```
   */
  async handleWebSocketUpgrade(request: Request): Promise<Response> {
    // Extract client info
    const clientId = request.headers.get('X-Client-ID') ?? generateUUID();
    const clientIp = request.headers.get('CF-Connecting-IP') ?? undefined;
    const priority = request.headers.get('X-Priority') ?? undefined;

    // Check connection rate limit
    const rateLimitResult = this.deps.rateLimiter.checkConnection(
      clientId,
      clientIp,
      priority
    );

    if (!rateLimitResult.allowed) {
      const headers = this.deps.rateLimiter.getRateLimitHeaders(rateLimitResult);

      // Return 429 for rate limited connections
      const body: Record<string, unknown> = {
        error: 'Too Many Requests',
        reason: rateLimitResult.reason,
        retryAfter: rateLimitResult.retryDelayMs
          ? Math.ceil(rateLimitResult.retryDelayMs / 1000)
          : 1,
      };

      if (rateLimitResult.clientIp) {
        body.clientIp = rateLimitResult.clientIp;
      }

      return new Response(JSON.stringify(body), {
        status: 429,
        headers: {
          'Content-Type': 'application/json',
          ...headers,
        },
      });
    }

    const webSocketPair = new WebSocketPair();
    const client = webSocketPair[0];
    const server = webSocketPair[1];

    const connectionId = generateUUID();
    const shardName = request.headers.get('X-Shard-Name') ?? undefined;

    // Create attachment for hibernation
    const attachment: ExtendedWebSocketAttachment = {
      sourceDoId: clientId,
      sourceShardName: shardName,
      lastAckSequence: 0,
      connectedAt: Date.now(),
      protocolVersion: 1,
      capabilityFlags: 0,
      clientIp,
      connectionId,
    };

    // Register connection with rate limiter
    this.deps.rateLimiter.registerConnection(connectionId, clientId, clientIp, priority);

    // Accept with hibernation support
    this.deps.ctx.acceptWebSocket(server, [clientId]);
    server.serializeAttachment(attachment);

    // Update buffer with new connection
    this.deps.buffer.updateSourceState(clientId, 0, 0, shardName);
    this.deps.buffer.registerSourceWebSocket(clientId, server);

    if (this.deps.getState() === 'idle') {
      this.deps.setState('receiving');
    }

    // Include rate limit headers in successful response
    const headers = this.deps.rateLimiter.getRateLimitHeaders(rateLimitResult);

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers,
    });
  }

  // ===========================================================================
  // WebSocket Message Handling
  // ===========================================================================

  /**
   * Handle incoming WebSocket message.
   * Acts as a dispatcher to focused handler methods.
   * Validates the connection, payload size, and rate limits before dispatching.
   *
   * @param ws - The WebSocket connection that received the message
   * @param message - The raw message data (string or binary)
   *
   * @example
   * ```typescript
   * // In a Durable Object webSocketMessage handler
   * async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
   *   await this.wsHandler.handleMessage(ws, message);
   * }
   * ```
   */
  async handleMessage(ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
    // Validate connection attachment
    const attachment = this.validateConnectionAttachment(ws);
    if (!attachment) {
      return;
    }

    // Calculate and validate payload size
    const payloadSize = this.calculatePayloadSize(message);
    if (!this.validatePayloadSize(ws, attachment, payloadSize)) {
      return;
    }

    // Parse, validate, and dispatch the message
    const handled = await this.parseAndDispatchMessage(ws, attachment, message, payloadSize);
    if (!handled) {
      return;
    }

    // Check if flush needed after message processing
    await this.checkAndScheduleFlush();
  }

  // ===========================================================================
  // Message Validation Methods
  // ===========================================================================

  /**
   * Validate and retrieve the connection attachment.
   * Closes the WebSocket if attachment is missing.
   */
  private validateConnectionAttachment(ws: WebSocket): ExtendedWebSocketAttachment | null {
    const attachment = this.deserializeAttachment(ws) as ExtendedWebSocketAttachment | null;
    if (!attachment) {
      ws.close(1008, 'Unknown connection');
      return null;
    }
    return attachment;
  }

  /**
   * Calculate the payload size in bytes.
   */
  private calculatePayloadSize(message: ArrayBuffer | string): number {
    return typeof message === 'string'
      ? new TextEncoder().encode(message).length
      : message.byteLength;
  }

  /**
   * Validate payload size before parsing.
   * Returns false if validation fails (NACK sent).
   */
  private validatePayloadSize(
    ws: WebSocket,
    attachment: ExtendedWebSocketAttachment,
    payloadSize: number
  ): boolean {
    // Check for empty message
    if (payloadSize === 0) {
      this.sendNack(ws, 0, 'invalid_format', 'Message is empty - no data received', false);
      return false;
    }

    // Pre-parse size validation: reject oversized messages BEFORE JSON.parse
    if (payloadSize > this.rateLimitConfig.maxPayloadSize) {
      this.handleOversizedPayload(ws, attachment, payloadSize);
      return false;
    }

    return true;
  }

  /**
   * Handle an oversized payload by tracking violations and sending NACK.
   */
  private handleOversizedPayload(
    ws: WebSocket,
    attachment: ExtendedWebSocketAttachment,
    payloadSize: number
  ): void {
    // Track size violation
    const connectionState = this.deps.rateLimiter['connections'].get(attachment.connectionId);
    if (connectionState) {
      connectionState.sizeViolationCount++;
    }
    this.deps.rateLimiter['metrics'].payloadRejections++;

    // Format human-readable size message
    const actualSizeMB = (payloadSize / (1024 * 1024)).toFixed(2);
    const maxSizeMB = (this.rateLimitConfig.maxPayloadSize / (1024 * 1024)).toFixed(2);

    // Send nack with payload_too_large reason BEFORE parsing
    this.sendNackOptions({
      ws,
      sequenceNumber: 0,
      reason: 'payload_too_large',
      errorMessage: `Payload size ${actualSizeMB} MB exceeded limit of ${maxSizeMB} MB`,
      shouldRetry: false,
      maxSize: this.rateLimitConfig.maxPayloadSize,
      actualSize: payloadSize,
    });

    // Check if connection should be closed due to violations
    if (this.deps.rateLimiter.shouldCloseConnection(attachment.connectionId)) {
      ws.close(1008, 'Too many size violations');
    }
  }

  // ===========================================================================
  // Message Parsing and Dispatch
  // ===========================================================================

  /**
   * Parse the message, check rate limits, and dispatch to appropriate handler.
   * Returns true if message was handled, false if an error occurred.
   */
  private async parseAndDispatchMessage(
    ws: WebSocket,
    attachment: ExtendedWebSocketAttachment,
    message: ArrayBuffer | string,
    payloadSize: number
  ): Promise<boolean> {
    try {
      const rpcMessage = this.decodeMessage(message);

      // Check rate limit and handle rejection if needed
      const rateLimitResult = this.checkMessageRateLimit(ws, attachment, rpcMessage, payloadSize);
      if (!rateLimitResult) {
        return false;
      }

      // Dispatch to appropriate message handler
      await this.dispatchMessage(ws, attachment, rpcMessage, rateLimitResult);
      return true;
    } catch (error) {
      this.handleMessageParseError(ws, error);
      return false;
    }
  }

  /**
   * Check message rate limit and send NACK if rejected.
   * Returns the rate limit result if allowed, null if rejected.
   */
  private checkMessageRateLimit(
    ws: WebSocket,
    attachment: ExtendedWebSocketAttachment,
    rpcMessage: ValidatedClientRpcMessage,
    payloadSize: number
  ): RateLimitResult | null {
    // Calculate event sizes for CDC batch (only after successful parse)
    let eventSizes: number[] = [];
    if (isCDCBatchMessage(rpcMessage)) {
      const cdcMessage = rpcMessage as CDCBatchMessage;
      eventSizes = cdcMessage.events.map((e) => JSON.stringify(e).length);
    }

    // Check message rate limit (excluding size check which was done pre-parse)
    const stats = this.deps.buffer.getStats();
    const rateLimitResult = this.deps.rateLimiter.checkMessage(
      attachment.connectionId,
      rpcMessage.type,
      payloadSize,
      eventSizes,
      stats.utilization
    );

    if (!rateLimitResult.allowed) {
      this.handleRateLimitRejection(ws, attachment, rpcMessage, rateLimitResult);
      return null;
    }

    return rateLimitResult;
  }

  /**
   * Handle a rate limit rejection by sending NACK and possibly closing connection.
   */
  private handleRateLimitRejection(
    ws: WebSocket,
    attachment: ExtendedWebSocketAttachment,
    rpcMessage: ValidatedClientRpcMessage,
    rateLimitResult: RateLimitResult
  ): void {
    const sequenceNumber = isCDCBatchMessage(rpcMessage)
      ? (rpcMessage as CDCBatchMessage).sequenceNumber
      : 0;

    this.sendNackOptions({
      ws,
      sequenceNumber,
      reason: rateLimitResult.reason || 'rate_limited',
      errorMessage: `Request ${rateLimitResult.reason || 'rate_limited'}`,
      shouldRetry: rateLimitResult.reason !== 'payload_too_large' &&
        rateLimitResult.reason !== 'event_too_large',
      retryDelayMs: rateLimitResult.retryDelayMs,
      rateLimit: rateLimitResult.rateLimit,
      maxSize: rateLimitResult.maxSize,
    });

    // Check if connection should be closed due to violations
    if (this.deps.rateLimiter.shouldCloseConnection(attachment.connectionId)) {
      ws.close(1008, 'Too many size violations');
    }
  }

  /**
   * Dispatch the parsed message to the appropriate handler based on message type.
   */
  private async dispatchMessage(
    ws: WebSocket,
    attachment: ExtendedWebSocketAttachment,
    rpcMessage: ValidatedClientRpcMessage,
    rateLimitResult: RateLimitResult
  ): Promise<void> {
    if (isCDCBatchMessage(rpcMessage)) {
      await this.handleCDCBatch(ws, attachment, rpcMessage as CDCBatchMessage, rateLimitResult);
    } else if (isConnectMessage(rpcMessage)) {
      await this.handleConnect(ws, attachment, rpcMessage as ConnectMessage);
    } else if (isHeartbeatMessage(rpcMessage)) {
      await this.handleHeartbeat(ws, attachment, rpcMessage as HeartbeatMessage);
    } else if (isFlushRequestMessage(rpcMessage)) {
      await this.handleFlushRequestMessage(ws, attachment, rpcMessage as FlushRequestMessage);
    }
  }

  /**
   * Handle errors that occur during message parsing or validation.
   */
  private handleMessageParseError(ws: WebSocket, error: unknown): void {
    console.error('Error handling message:', error);
    const errorMessage = error instanceof MessageValidationError
      ? error.getErrorDetails()
      : String(error);
    this.sendNack(ws, 0, 'invalid_format', errorMessage, false);
  }

  /**
   * Check if a flush is needed and schedule it.
   */
  private async checkAndScheduleFlush(): Promise<void> {
    const trigger = this.deps.buffer.shouldFlush();
    if (trigger) {
      await this.deps.scheduleFlush(trigger);
    }
  }

  /**
   * Handle WebSocket close.
   * Unregisters the connection, cleans up rate limiter state, and triggers a flush
   * if this was the last connection and the buffer has pending events.
   *
   * @param ws - The WebSocket connection that closed
   * @param code - The WebSocket close code
   * @param reason - The close reason string
   * @param wasClean - Whether the close was clean
   *
   * @example
   * ```typescript
   * // In a Durable Object webSocketClose handler
   * async webSocketClose(
   *   ws: WebSocket,
   *   code: number,
   *   reason: string,
   *   wasClean: boolean
   * ): Promise<void> {
   *   await this.wsHandler.handleClose(ws, code, reason, wasClean);
   * }
   * ```
   */
  async handleClose(
    ws: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean
  ): Promise<void> {
    const attachment = this.deserializeAttachment(ws) as ExtendedWebSocketAttachment | null;
    if (attachment) {
      this.deps.buffer.unregisterSourceWebSocket(attachment.sourceDoId);

      // Unregister from rate limiter
      this.deps.rateLimiter.unregisterConnection(
        attachment.connectionId,
        attachment.sourceDoId,
        attachment.clientIp
      );

      console.log(
        `Source ${attachment.sourceDoId} disconnected: code=${code}, reason=${reason}, clean=${wasClean}`
      );
    }

    // Flush if no more connections
    const sockets = this.deps.ctx.getWebSockets();
    if (sockets.length === 0 && this.deps.buffer.getStats().eventCount > 0) {
      await this.deps.flush('shutdown');
    }
  }

  /**
   * Handle WebSocket error.
   * Logs the error and cleans up the connection state.
   *
   * @param ws - The WebSocket connection that encountered an error
   * @param error - The error that occurred
   *
   * @example
   * ```typescript
   * // In a Durable Object webSocketError handler
   * async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
   *   await this.wsHandler.handleError(ws, error);
   * }
   * ```
   */
  async handleError(ws: WebSocket, error: unknown): Promise<void> {
    console.error('WebSocket error:', error);
    const attachment = this.deserializeAttachment(ws) as ExtendedWebSocketAttachment | null;
    if (attachment) {
      this.deps.buffer.unregisterSourceWebSocket(attachment.sourceDoId);

      // Unregister from rate limiter
      this.deps.rateLimiter.unregisterConnection(
        attachment.connectionId,
        attachment.sourceDoId,
        attachment.clientIp
      );
    }
  }

  // ===========================================================================
  // Message Handlers
  // ===========================================================================

  /**
   * Handle CDC batch message
   */
  private async handleCDCBatch(
    ws: WebSocket,
    attachment: WebSocketAttachment,
    message: CDCBatchMessage,
    rateLimitResult?: RateLimitResult
  ): Promise<void> {
    this.deps.setState('receiving');

    try {
      const result = this.deps.buffer.addBatch(
        message.sourceDoId,
        message.events,
        message.sequenceNumber,
        message.sourceShardName
      );

      if (result.isDuplicate) {
        this.sendAck(
          ws,
          message.sequenceNumber,
          'duplicate',
          message.correlationId,
          undefined,
          rateLimitResult?.rateLimit
        );
        return;
      }

      if (!result.added) {
        this.sendNack(
          ws,
          message.sequenceNumber,
          'internal_error',
          'Failed to add batch',
          true
        );
        return;
      }

      // Update attachment
      attachment.lastAckSequence = message.sequenceNumber;
      ws.serializeAttachment(attachment);

      // Determine status
      const stats = this.deps.buffer.getStats();
      const status: AckMessage['status'] = stats.utilization > 0.8 ? 'buffered' : 'ok';

      this.sendAck(ws, message.sequenceNumber, status, message.correlationId, {
        eventsProcessed: message.events.length,
        bufferUtilization: stats.utilization,
        timeUntilFlush: this.deps.buffer.getTimeUntilFlush(),
        remainingTokens: rateLimitResult?.remainingTokens,
        bucketCapacity: rateLimitResult?.bucketCapacity,
        suggestedDelayMs: rateLimitResult?.suggestedDelayMs,
        circuitBreakerState: this.deps.getCircuitBreakerState(),
      }, rateLimitResult?.rateLimit);
    } catch (error) {
      if (error instanceof BufferOverflowError) {
        this.sendNackOptions({
          ws,
          sequenceNumber: message.sequenceNumber,
          reason: 'buffer_full',
          errorMessage: error.message,
          shouldRetry: true,
          retryDelayMs: 5000,
          rateLimit: rateLimitResult?.rateLimit,
          // Include additional context in a custom field for advanced clients
          maxSize: error.maxSizeBytes,
        });
      } else {
        this.sendNack(
          ws,
          message.sequenceNumber,
          'internal_error',
          String(error),
          true
        );
      }
    }
  }

  /**
   * Handle connect message
   */
  private async handleConnect(
    ws: WebSocket,
    attachment: WebSocketAttachment,
    message: ConnectMessage
  ): Promise<void> {
    attachment.sourceDoId = message.sourceDoId;
    attachment.sourceShardName = message.sourceShardName;
    attachment.lastAckSequence = message.lastAckSequence;
    attachment.protocolVersion = message.protocolVersion;
    attachment.capabilityFlags = encodeCapabilities(message.capabilities);
    ws.serializeAttachment(attachment);

    this.deps.buffer.updateSourceState(
      message.sourceDoId,
      0,
      message.lastAckSequence,
      message.sourceShardName
    );
    this.deps.buffer.registerSourceWebSocket(message.sourceDoId, ws);

    this.sendStatus(ws);
  }

  /**
   * Handle heartbeat message
   */
  private async handleHeartbeat(
    ws: WebSocket,
    attachment: WebSocketAttachment,
    message: HeartbeatMessage
  ): Promise<void> {
    this.deps.buffer.updateSourceState(message.sourceDoId, 0, message.lastAckSequence);
    attachment.lastAckSequence = message.lastAckSequence;
    ws.serializeAttachment(attachment);

    ws.send(serialize({
      type: 'pong',
      timestamp: message.timestamp,
      serverTime: Date.now(),
    }));
  }

  /**
   * Handle flush request message
   */
  private async handleFlushRequestMessage(
    ws: WebSocket,
    _attachment: WebSocketAttachment,
    message: FlushRequestMessage
  ): Promise<void> {
    const trigger = message.reason === 'manual' ? 'manual' : 'threshold_events';
    const result = await this.deps.flush(trigger);

    ws.send(serialize({
      type: 'flush_response',
      timestamp: Date.now(),
      correlationId: message.correlationId,
      result,
    }));
  }

  // ===========================================================================
  // Response Helpers
  // ===========================================================================

  /**
   * Send an ACK message to the WebSocket client.
   * Consolidated helper that handles both basic ACKs and ACKs with rate limit info.
   */
  private sendAck(
    ws: WebSocket,
    sequenceNumber: number,
    status: AckMessage['status'],
    correlationId?: string,
    details?: AckMessage['details'],
    rateLimit?: RateLimitInfo
  ): void {
    const message: AckMessage = {
      type: 'ack',
      timestamp: Date.now(),
      correlationId,
      sequenceNumber,
      status,
      details,
      rateLimit,
    };
    ws.send(serialize(message));
  }

  /**
   * Options for sendNack to consolidate all NACK variants.
   */
  private sendNackOptions(options: {
    ws: WebSocket;
    sequenceNumber: number;
    reason: NackMessage['reason'];
    errorMessage: string;
    shouldRetry: boolean;
    retryDelayMs?: number | undefined;
    rateLimit?: RateLimitInfo | undefined;
    maxSize?: number | undefined;
    actualSize?: number | undefined;
  }): void {
    const message: NackMessage & { rateLimit?: RateLimitInfo; actualSize?: number; receivedSize?: number } = {
      type: 'nack',
      timestamp: Date.now(),
      sequenceNumber: options.sequenceNumber,
      reason: options.reason,
      errorMessage: options.errorMessage,
      shouldRetry: options.shouldRetry,
    };
    if (options.retryDelayMs !== undefined) {
      message.retryDelayMs = options.retryDelayMs;
    }
    if (options.maxSize !== undefined) {
      message.maxSize = options.maxSize;
    }
    if (options.rateLimit) {
      message.rateLimit = options.rateLimit;
    }
    if (options.actualSize !== undefined) {
      message.actualSize = options.actualSize;
      message.receivedSize = options.actualSize;
    }
    options.ws.send(serialize(message));
  }

  /**
   * Send a NACK message to the WebSocket client.
   * Public method for basic NACK sending (maintains backward compatibility).
   *
   * @param ws - The WebSocket connection to send the NACK to
   * @param sequenceNumber - The sequence number of the rejected message
   * @param reason - The reason for the rejection
   * @param errorMessage - Human-readable error description
   * @param shouldRetry - Whether the client should retry the operation
   * @param retryDelayMs - Optional delay before retrying (in milliseconds)
   *
   * @example
   * ```typescript
   * // Reject a message due to validation failure
   * handler.sendNack(
   *   ws,
   *   message.sequenceNumber,
   *   'invalid_format',
   *   'Missing required field: events',
   *   false
   * );
   *
   * // Reject with retry suggestion
   * handler.sendNack(
   *   ws,
   *   message.sequenceNumber,
   *   'buffer_full',
   *   'Buffer is at capacity',
   *   true,
   *   5000 // retry after 5 seconds
   * );
   * ```
   */
  sendNack(
    ws: WebSocket,
    sequenceNumber: number,
    reason: NackMessage['reason'],
    errorMessage: string,
    shouldRetry: boolean,
    retryDelayMs?: number
  ): void {
    this.sendNackOptions({
      ws,
      sequenceNumber,
      reason,
      errorMessage,
      shouldRetry,
      retryDelayMs,
    });
  }

  /**
   * Send a status message to the WebSocket client.
   * Includes current state, buffer statistics, and connection info.
   *
   * @param ws - The WebSocket connection to send the status to
   *
   * @example
   * ```typescript
   * // Send status after a connect message
   * handler.sendStatus(ws);
   *
   * // Send periodic status updates
   * for (const ws of ctx.getWebSockets()) {
   *   handler.sendStatus(ws);
   * }
   * ```
   */
  sendStatus(ws: WebSocket): void {
    const stats = this.deps.buffer.getStats();
    const sockets = this.deps.ctx.getWebSockets();

    const message: StatusMessage = {
      type: 'status',
      timestamp: Date.now(),
      state: this.deps.getState(),
      buffer: stats,
      connectedSources: sockets.length,
      lastFlushTime: undefined,
      nextFlushTime: Date.now() + this.deps.buffer.getTimeUntilFlush(),
    };
    ws.send(serialize(message));
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Decode and validate a WebSocket message using Zod schemas.
   */
  private decodeMessage(message: ArrayBuffer | string): ValidatedClientRpcMessage {
    // Parse JSON with bigint support
    let raw: unknown;
    try {
      if (typeof message === 'string') {
        raw = JSON.parse(message, bigintReviver);
      } else {
        raw = JSON.parse(new TextDecoder().decode(message), bigintReviver);
      }
    } catch (error) {
      throw new MessageValidationError(
        `Failed to parse JSON: ${error instanceof Error ? error.message : String(error)}`,
        null
      );
    }

    // Validate with Zod schema
    const result = ClientRpcMessageSchema.safeParse(raw);

    if (!result.success) {
      throw new MessageValidationError(
        `Invalid message format: ${result.error.issues.map((e) => e.message).join(', ')}`,
        result.error
      );
    }

    return result.data;
  }

  private deserializeAttachment(ws: WebSocket): WebSocketAttachment | null {
    try {
      return ws.deserializeAttachment() as WebSocketAttachment;
    } catch {
      return null;
    }
  }
}

// =============================================================================
// Hibernation Support
// =============================================================================

/**
 * Restore buffer state from hibernation
 */
export async function restoreFromHibernation(
  ctx: DurableObjectState,
  buffer: CDCBufferManager,
  config: { maxBatchSize?: number; flushThresholdBytes?: number; flushThresholdEvents?: number }
): Promise<CDCBufferManager> {
  try {
    const snapshot = await ctx.storage.get<BufferSnapshot>('buffer_snapshot');
    if (snapshot) {
      const restoredBuffer = CDCBufferManager.restore(snapshot, config);
      await ctx.storage.delete('buffer_snapshot');
      return restoredBuffer;
    }
  } catch {
    // Start fresh
  }

  // Restore WebSocket connections
  const sockets = ctx.getWebSockets();
  for (const ws of sockets) {
    try {
      const attachment = ws.deserializeAttachment() as WebSocketAttachment;
      if (attachment) {
        buffer.updateSourceState(
          attachment.sourceDoId,
          0,
          attachment.lastAckSequence,
          attachment.sourceShardName
        );
        buffer.registerSourceWebSocket(attachment.sourceDoId, ws);
      }
    } catch {
      // Skip invalid connections
    }
  }

  return buffer;
}

/**
 * Setup auto-response for ping/pong
 */
export function setupWebSocketAutoResponse(ctx: DurableObjectState): void {
  ctx.setWebSocketAutoResponse(
    new WebSocketRequestResponsePair('ping', 'pong')
  );
}
