/**
 * DoSQL RPC Client
 *
 * Client implementation for connecting to DoSQL Durable Objects via CapnWeb RPC.
 * Supports both WebSocket (for streaming) and HTTP batch modes.
 *
 * @packageDocumentation
 */

import {
  newWebSocketRpcSession,
  newHttpBatchRpcSession,
  type RpcStub,
} from 'capnweb';

import type {
  DoSQLAPI,
  QueryRequest,
  QueryResponse,
  StreamRequest,
  StreamChunk,
  StreamComplete,
  CDCRequest,
  CDCEvent,
  CDCAck,
  BeginTransactionRequest,
  TransactionHandle,
  CommitRequest,
  RollbackRequest,
  TransactionResult,
  BatchRequest,
  BatchResponse,
  SchemaRequest,
  SchemaResponse,
  ConnectionOptions,
  ConnectionStats,
  RPCError,
  RPCErrorCode,
} from './types.js';

// =============================================================================
// Client Types
// =============================================================================

/**
 * DoSQL client interface
 *
 * Provides type-safe access to DoSQL RPC methods with
 * automatic connection management.
 */
export interface DoSQLClient extends DoSQLAPI {
  /** Close the connection */
  close(): void;
  /** Check if connected */
  isConnected(): boolean;
  /** Get current connection statistics */
  getConnectionStats(): ConnectionStats;
  /** Reconnect to the server */
  reconnect(): Promise<void>;
}

/**
 * Transaction context for executing queries within a transaction
 */
export interface TransactionContext {
  /** Transaction ID */
  readonly txId: string;
  /** Execute a query within this transaction */
  query(request: Omit<QueryRequest, 'txId'>): Promise<QueryResponse>;
  /** Commit the transaction */
  commit(): Promise<TransactionResult>;
  /** Rollback the transaction */
  rollback(savepoint?: string): Promise<TransactionResult>;
  /** Create a savepoint */
  savepoint(name: string): Promise<void>;
}

// =============================================================================
// Client State
// =============================================================================

interface ClientState {
  connected: boolean;
  connectionId?: string;
  currentLSN?: bigint;
  latencyMs?: number;
  messagesSent: number;
  messagesReceived: number;
  reconnectCount: number;
  cdcSubscriptions: Map<string, { unsubscribe: () => void }>;
}

// =============================================================================
// Create Client Functions
// =============================================================================

/**
 * Create a DoSQL client using WebSocket transport
 *
 * WebSocket transport is recommended for:
 * - Streaming query results
 * - CDC subscriptions
 * - Long-running connections
 *
 * @param options - Connection options
 * @returns DoSQL client instance
 *
 * @example
 * ```typescript
 * const client = createWebSocketClient({
 *   url: 'wss://dosql.example.com/rpc',
 *   defaultBranch: 'main',
 *   autoReconnect: true,
 * });
 *
 * // Execute a query
 * const result = await client.query({
 *   sql: 'SELECT * FROM users WHERE id = $1',
 *   params: [123],
 * });
 *
 * // Subscribe to changes
 * for await (const event of client.subscribeCDC({ fromLSN: 0n, tables: ['users'] })) {
 *   console.log('Change:', event);
 * }
 *
 * // Clean up
 * client.close();
 * ```
 */
export function createWebSocketClient(options: ConnectionOptions): DoSQLClient {
  const {
    url,
    defaultBranch,
    connectTimeoutMs = 30000,
    queryTimeoutMs = 30000,
    autoReconnect = true,
    maxReconnectAttempts = 5,
    reconnectDelayMs = 1000,
  } = options;

  // Client state
  const state: ClientState = {
    connected: false,
    messagesSent: 0,
    messagesReceived: 0,
    reconnectCount: 0,
    cdcSubscriptions: new Map(),
  };

  // Create CapnWeb WebSocket session
  let session: RpcStub<DoSQLAPI> = newWebSocketRpcSession<DoSQLAPI>(url);
  state.connected = true;

  // Wrap method to add default branch and track stats
  function wrapRequest<T extends { branch?: string }>(request: T): T {
    state.messagesSent++;
    if (defaultBranch && !request.branch) {
      return { ...request, branch: defaultBranch };
    }
    return request;
  }

  // Track response and update state
  function trackResponse<T>(promise: Promise<T>): Promise<T> {
    return promise.then((result) => {
      state.messagesReceived++;
      // Extract LSN if present
      const r = result as unknown as { lsn?: bigint };
      if (r.lsn !== undefined) {
        state.currentLSN = r.lsn;
      }
      return result;
    });
  }

  // Create CDC subscription async iterator
  async function* createCDCIterator(request: CDCRequest): AsyncIterable<CDCEvent> {
    const subscriptionId = crypto.randomUUID();

    // This is a simplified implementation
    // In a real implementation, the server would push events
    // via the WebSocket connection

    // Set up subscription on server
    const ack = await (session as unknown as {
      _subscribeCDC(request: CDCRequest & { subscriptionId: string }): Promise<CDCAck>;
    })._subscribeCDC({
      ...wrapRequest(request),
      subscriptionId,
    });

    state.cdcSubscriptions.set(subscriptionId, {
      unsubscribe: () => {
        // Clean up subscription
        void (session as unknown as {
          _unsubscribeCDC(id: string): Promise<void>;
        })._unsubscribeCDC(subscriptionId);
      },
    });

    try {
      // In a real implementation, this would receive events from the server
      // For now, we simulate with a polling approach
      while (state.connected) {
        // The actual implementation would receive pushed events
        // This is just a placeholder showing the iteration pattern
        const events = await (session as unknown as {
          _pollCDC(subscriptionId: string): Promise<CDCEvent[]>;
        })._pollCDC(subscriptionId);

        for (const event of events) {
          yield event;
        }

        // Small delay to prevent tight loop in this mock implementation
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    } finally {
      // Clean up subscription
      state.cdcSubscriptions.get(subscriptionId)?.unsubscribe();
      state.cdcSubscriptions.delete(subscriptionId);
    }
  }

  // Create streaming query async iterator
  async function* createStreamIterator(request: StreamRequest): AsyncIterable<StreamChunk> {
    const streamId = crypto.randomUUID();

    // Initialize stream on server
    const initResult = await (session as unknown as {
      _initStream(request: StreamRequest & { streamId: string }): Promise<{
        columns: string[];
        columnTypes: string[];
      }>;
    })._initStream({
      ...wrapRequest(request),
      streamId,
    });

    try {
      let isComplete = false;

      while (!isComplete) {
        // Fetch next chunk
        const chunk = await (session as unknown as {
          _nextChunk(streamId: string): Promise<StreamChunk>;
        })._nextChunk(streamId);

        yield chunk;
        isComplete = chunk.isLast;
      }
    } finally {
      // Clean up stream
      await (session as unknown as {
        _closeStream(streamId: string): Promise<void>;
      })._closeStream(streamId).catch(() => {
        // Ignore cleanup errors
      });
    }
  }

  // Build client implementation
  const client: DoSQLClient = {
    // Query operations
    async query(request: QueryRequest): Promise<QueryResponse> {
      return trackResponse(session.query(wrapRequest(request)));
    },

    queryStream(request: StreamRequest): AsyncIterable<StreamChunk> {
      return createStreamIterator(wrapRequest(request));
    },

    // Transaction operations
    async beginTransaction(request: BeginTransactionRequest): Promise<TransactionHandle> {
      return trackResponse(session.beginTransaction(wrapRequest(request)));
    },

    async commit(request: CommitRequest): Promise<TransactionResult> {
      return trackResponse(session.commit(request));
    },

    async rollback(request: RollbackRequest): Promise<TransactionResult> {
      return trackResponse(session.rollback(request));
    },

    // Batch operations
    async batch(request: BatchRequest): Promise<BatchResponse> {
      return trackResponse(session.batch(wrapRequest(request)));
    },

    // CDC operations
    subscribeCDC(request: CDCRequest): AsyncIterable<CDCEvent> {
      return createCDCIterator(wrapRequest(request));
    },

    async unsubscribeCDC(subscriptionId: string): Promise<void> {
      const subscription = state.cdcSubscriptions.get(subscriptionId);
      if (subscription) {
        subscription.unsubscribe();
        state.cdcSubscriptions.delete(subscriptionId);
      }
      return session.unsubscribeCDC(subscriptionId);
    },

    // Schema operations
    async getSchema(request: SchemaRequest): Promise<SchemaResponse> {
      return trackResponse(session.getSchema(wrapRequest(request)));
    },

    // Connection operations
    async ping(): Promise<{ pong: true; lsn: bigint; timestamp: number }> {
      const start = performance.now();
      const result = await session.ping();
      state.latencyMs = performance.now() - start;
      return result;
    },

    async getStats(): Promise<ConnectionStats> {
      return session.getStats();
    },

    // Client-specific operations
    close(): void {
      state.connected = false;

      // Clean up all CDC subscriptions
      for (const [id, sub] of state.cdcSubscriptions) {
        sub.unsubscribe();
        state.cdcSubscriptions.delete(id);
      }

      // Dispose the CapnWeb session
      // CapnWeb stubs implement Disposable
      const disposable = session as unknown as { dispose?: () => void };
      if (typeof disposable.dispose === 'function') {
        disposable.dispose();
      }
    },

    isConnected(): boolean {
      return state.connected;
    },

    getConnectionStats(): ConnectionStats {
      return {
        connected: state.connected,
        connectionId: state.connectionId,
        branch: defaultBranch,
        currentLSN: state.currentLSN,
        latencyMs: state.latencyMs,
        messagesSent: state.messagesSent,
        messagesReceived: state.messagesReceived,
        reconnectCount: state.reconnectCount,
      };
    },

    async reconnect(): Promise<void> {
      // Close existing connection
      client.close();

      // Reconnect
      session = newWebSocketRpcSession<DoSQLAPI>(url);
      state.connected = true;
      state.reconnectCount++;
    },
  };

  return client;
}

/**
 * Create a DoSQL client using HTTP batch transport
 *
 * HTTP batch transport is recommended for:
 * - Stateless requests
 * - Serverless environments
 * - Simple query execution
 *
 * Note: HTTP batch does not support streaming or CDC subscriptions.
 *
 * @param options - Connection options
 * @returns DoSQL client instance
 *
 * @example
 * ```typescript
 * const client = createHttpClient({
 *   url: 'https://dosql.example.com/rpc',
 *   defaultBranch: 'main',
 * });
 *
 * // Execute a query
 * const result = await client.query({
 *   sql: 'SELECT * FROM users LIMIT 10',
 * });
 *
 * // Close is a no-op for HTTP but good practice
 * client.close();
 * ```
 */
export function createHttpClient(options: ConnectionOptions): DoSQLClient {
  const { url, defaultBranch } = options;

  // Client state
  const state: ClientState = {
    connected: true, // HTTP is always "connected"
    messagesSent: 0,
    messagesReceived: 0,
    reconnectCount: 0,
    cdcSubscriptions: new Map(),
  };

  // Create CapnWeb HTTP batch session
  const session: RpcStub<DoSQLAPI> = newHttpBatchRpcSession<DoSQLAPI>(url);

  // Wrap request with defaults
  function wrapRequest<T extends { branch?: string }>(request: T): T {
    state.messagesSent++;
    if (defaultBranch && !request.branch) {
      return { ...request, branch: defaultBranch };
    }
    return request;
  }

  // Track response
  function trackResponse<T>(promise: Promise<T>): Promise<T> {
    return promise.then((result) => {
      state.messagesReceived++;
      const r = result as unknown as { lsn?: bigint };
      if (r.lsn !== undefined) {
        state.currentLSN = r.lsn;
      }
      return result;
    });
  }

  const client: DoSQLClient = {
    // Query operations
    async query(request: QueryRequest): Promise<QueryResponse> {
      return trackResponse(session.query(wrapRequest(request)));
    },

    queryStream(_request: StreamRequest): AsyncIterable<StreamChunk> {
      throw new Error('Streaming not supported in HTTP batch mode. Use WebSocket client instead.');
    },

    // Transaction operations
    async beginTransaction(request: BeginTransactionRequest): Promise<TransactionHandle> {
      return trackResponse(session.beginTransaction(wrapRequest(request)));
    },

    async commit(request: CommitRequest): Promise<TransactionResult> {
      return trackResponse(session.commit(request));
    },

    async rollback(request: RollbackRequest): Promise<TransactionResult> {
      return trackResponse(session.rollback(request));
    },

    // Batch operations
    async batch(request: BatchRequest): Promise<BatchResponse> {
      return trackResponse(session.batch(wrapRequest(request)));
    },

    // CDC operations
    subscribeCDC(_request: CDCRequest): AsyncIterable<CDCEvent> {
      throw new Error('CDC subscriptions not supported in HTTP batch mode. Use WebSocket client instead.');
    },

    async unsubscribeCDC(_subscriptionId: string): Promise<void> {
      throw new Error('CDC subscriptions not supported in HTTP batch mode. Use WebSocket client instead.');
    },

    // Schema operations
    async getSchema(request: SchemaRequest): Promise<SchemaResponse> {
      return trackResponse(session.getSchema(wrapRequest(request)));
    },

    // Connection operations
    async ping(): Promise<{ pong: true; lsn: bigint; timestamp: number }> {
      const start = performance.now();
      const result = await session.ping();
      state.latencyMs = performance.now() - start;
      return result;
    },

    async getStats(): Promise<ConnectionStats> {
      return session.getStats();
    },

    // Client-specific operations
    close(): void {
      // HTTP batch mode doesn't maintain a persistent connection
      // but we update state for consistency
      state.connected = false;
    },

    isConnected(): boolean {
      return state.connected;
    },

    getConnectionStats(): ConnectionStats {
      return {
        connected: state.connected,
        branch: defaultBranch,
        currentLSN: state.currentLSN,
        latencyMs: state.latencyMs,
        messagesSent: state.messagesSent,
        messagesReceived: state.messagesReceived,
        reconnectCount: state.reconnectCount,
      };
    },

    async reconnect(): Promise<void> {
      // HTTP is stateless, nothing to reconnect
      state.connected = true;
    },
  };

  return client;
}

// =============================================================================
// Transaction Helper
// =============================================================================

/**
 * Execute a function within a transaction context
 *
 * Automatically handles commit on success and rollback on error.
 *
 * @param client - DoSQL client
 * @param fn - Function to execute within transaction
 * @param options - Transaction options
 * @returns Result of the function
 *
 * @example
 * ```typescript
 * const result = await withTransaction(client, async (tx) => {
 *   await tx.query({ sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] });
 *   await tx.query({ sql: 'INSERT INTO logs (action) VALUES ($1)', params: ['user_created'] });
 *   return { success: true };
 * });
 * ```
 */
export async function withTransaction<T>(
  client: DoSQLClient,
  fn: (tx: TransactionContext) => Promise<T>,
  options: BeginTransactionRequest = {}
): Promise<T> {
  const handle = await client.beginTransaction(options);

  const tx: TransactionContext = {
    txId: handle.txId,

    async query(request) {
      return client.query({ ...request, branch: options.branch });
    },

    async commit() {
      return client.commit({ txId: handle.txId });
    },

    async rollback(savepoint) {
      return client.rollback({ txId: handle.txId, savepoint });
    },

    async savepoint(name) {
      await client.query({
        sql: `SAVEPOINT ${name}`,
        branch: options.branch,
      });
    },
  };

  try {
    const result = await fn(tx);
    await tx.commit();
    return result;
  } catch (error) {
    await tx.rollback().catch(() => {
      // Ignore rollback errors
    });
    throw error;
  }
}

// =============================================================================
// Error Handling Utilities
// =============================================================================

/**
 * Check if an error is an RPC error
 */
export function isRPCError(error: unknown): error is RPCError {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    'message' in error
  );
}

/**
 * Create a typed RPC error
 */
export function createRPCError(
  code: RPCErrorCode,
  message: string,
  details?: Record<string, unknown>
): RPCError {
  return { code, message, details };
}

// =============================================================================
// Re-exports for convenience
// =============================================================================

export type {
  DoSQLAPI,
  QueryRequest,
  QueryResponse,
  StreamRequest,
  StreamChunk,
  CDCRequest,
  CDCEvent,
  TransactionHandle,
  TransactionResult,
  BatchRequest,
  BatchResponse,
  SchemaRequest,
  SchemaResponse,
  ConnectionOptions,
  ConnectionStats,
  RPCError,
};

export { RPCErrorCode } from './types.js';
