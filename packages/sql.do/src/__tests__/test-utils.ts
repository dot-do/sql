/**
 * Test Utilities for sql.do Package
 *
 * This module provides type-safe test helpers, mock factories, and fixture creators
 * to replace `as any` type assertions in test files.
 *
 * @packageDocumentation
 */

import type {
  SQLClient,
  QueryResult,
  SQLValue,
  TransactionContext,
  TransactionState,
  TransactionId,
  LSN,
  IdempotencyCacheStats,
  PoolStats,
  PoolConfig,
  ClientEventMap,
  ClientEventListener,
  ConnectedEvent,
  DisconnectedEvent,
  ClientErrorEvent,
} from '../types.js';
import type { SQLClientConfig } from '../client.js';
import { DoSQLClient, createSQLClient } from '../client.js';
import { createTransactionId, createLSN } from '../types.js';

// =============================================================================
// Mock WebSocket
// =============================================================================

/**
 * WebSocket event listener callback type
 */
type WebSocketEventCallback = (event: WebSocketEventData) => void;

/**
 * WebSocket event data structure
 */
interface WebSocketEventData {
  data?: string;
  error?: Error;
  code?: number;
  reason?: string;
}

/**
 * WebSocket ready state constants
 */
export const WebSocketReadyState = {
  CONNECTING: 0,
  OPEN: 1,
  CLOSING: 2,
  CLOSED: 3,
} as const;

/**
 * Fake WebSocket implementation for testing.
 * This is a TEST DOUBLE (not a mock) - it implements real WebSocket behavior
 * in-memory for testing without network calls.
 *
 * Per NO MOCKS philosophy: This is a valid test double that provides real
 * behavior (event emission, state tracking) rather than mocked return values.
 *
 * @deprecated Alias for FakeWebSocket - use FakeWebSocket for clarity
 */
export class MockWebSocket {
  static READY_STATE_CONNECTING = WebSocketReadyState.CONNECTING;
  static READY_STATE_OPEN = WebSocketReadyState.OPEN;
  static READY_STATE_CLOSING = WebSocketReadyState.CLOSING;
  static READY_STATE_CLOSED = WebSocketReadyState.CLOSED;

  readyState: number = WebSocketReadyState.CONNECTING;
  url: string;
  id: string;
  createdAt: number;
  lastUsedAt: number;
  private listeners = new Map<string, Set<WebSocketEventCallback>>();

  constructor(url: string) {
    this.url = url;
    this.id = `ws-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    this.createdAt = Date.now();
    this.lastUsedAt = Date.now();

    // Simulate async connection
    setTimeout(() => {
      this.readyState = WebSocketReadyState.OPEN;
      this.emit('open', {});
    }, 10);
  }

  addEventListener(event: string, callback: WebSocketEventCallback): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(callback);
  }

  removeEventListener(event: string, callback: WebSocketEventCallback): void {
    this.listeners.get(event)?.delete(callback);
  }

  send(data: string): void {
    this.lastUsedAt = Date.now();
    const request = JSON.parse(data) as { id: string };
    setTimeout(() => {
      this.emit('message', {
        data: JSON.stringify({
          id: request.id,
          result: { rows: [], rowsAffected: 0 },
        }),
      });
    }, 5);
  }

  close(): void {
    this.readyState = WebSocketReadyState.CLOSED;
    this.emit('close', {});
  }

  private emit(event: string, data: WebSocketEventData): void {
    this.listeners.get(event)?.forEach((callback) => callback(data));
  }

  // Test helpers
  simulateError(error: Error): void {
    this.emit('error', { error });
  }

  simulateClose(code?: number, reason?: string): void {
    this.readyState = WebSocketReadyState.CLOSED;
    const data: WebSocketEventData = {};
    // Only add optional properties if defined (for exactOptionalPropertyTypes)
    if (code !== undefined) data.code = code;
    if (reason !== undefined) data.reason = reason;
    this.emit('close', data);
  }

  simulateMessage(data: unknown): void {
    this.emit('message', { data: JSON.stringify(data) });
  }

  simulateOpen(): void {
    this.readyState = WebSocketReadyState.OPEN;
    this.emit('open', {});
  }
}

/**
 * Alias for MockWebSocket - preferred name to indicate this is a test double
 * (fake), not a mock per NO MOCKS philosophy.
 */
export { MockWebSocket as FakeWebSocket };

/**
 * Container for tracking FakeWebSocket instances in tests.
 * @deprecated Use FakeWebSocketTracker for clarity
 */
export class MockWebSocketTracker {
  instances: MockWebSocket[] = [];

  /**
   * Creates a MockWebSocket class that tracks instances
   */
  createTrackedClass(): typeof MockWebSocket {
    const tracker = this;
    return class TrackedMockWebSocket extends MockWebSocket {
      constructor(url: string) {
        super(url);
        tracker.instances.push(this);
      }
    };
  }

  /**
   * Gets the most recent WebSocket instance
   */
  get latest(): MockWebSocket | undefined {
    return this.instances[this.instances.length - 1];
  }

  /**
   * Clears all tracked instances
   */
  clear(): void {
    this.instances = [];
  }
}

/**
 * Alias for MockWebSocketTracker - preferred name to indicate this tracks
 * test doubles (fakes), not mocks per NO MOCKS philosophy.
 */
export { MockWebSocketTracker as FakeWebSocketTracker };

// =============================================================================
// Global WebSocket Test Double Setup
// =============================================================================

/**
 * Type for the globalThis object with WebSocket property
 */
interface GlobalWithWebSocket {
  WebSocket: typeof MockWebSocket | typeof WebSocket;
}

/**
 * Gets the global object with proper typing for WebSocket access
 */
export function getGlobalWithWebSocket(): GlobalWithWebSocket {
  return globalThis as unknown as GlobalWithWebSocket;
}

/**
 * Sets up the mock WebSocket on globalThis and returns a cleanup function.
 * @param MockClass - The mock WebSocket class to use (defaults to MockWebSocket)
 * @returns Cleanup function that restores the original WebSocket
 */
export function setupMockWebSocket(
  MockClass: typeof MockWebSocket = MockWebSocket
): () => void {
  const global = getGlobalWithWebSocket();
  const original = global.WebSocket;
  global.WebSocket = MockClass;
  return () => {
    global.WebSocket = original;
  };
}

// =============================================================================
// Client Fixtures
// =============================================================================

/**
 * Extended idempotency config with GAP features
 */
interface ExtendedIdempotencyConfig {
  enabled: boolean;
  keyPrefix?: string;
  maxCacheSize?: number;
  cacheTtlMs?: number;
  cleanupIntervalMs?: number;
  ttlMs?: number;
  // GAP features
  onEviction?: (event: EvictionEvent) => void;
  keyGenerator?: (sql: string, params?: SQLValue[]) => Promise<string>;
  keyStorage?: {
    get: (key: string) => string | undefined;
    set: (key: string, value: string) => void;
    delete: (key: string) => boolean;
  };
}

/**
 * Extended client config that includes all possible test configurations.
 * Use this instead of `config as any` for testing GAP features.
 */
export interface TestClientConfig extends Omit<SQLClientConfig, 'idempotency'> {
  // GAP features that may or may not exist
  eagerConnect?: boolean;
  autoReconnect?: boolean;
  connectionTimeout?: number;
  idleTimeout?: number;
  queueWhileReconnecting?: boolean;
  reconnect?: {
    maxAttempts?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
  };
  keepAlive?: {
    interval?: number;
    timeout?: number;
  };
  streamingTimeout?: number;
  readReplicas?: string[];
  pool?: PoolConfig;
  hibernation?: {
    enabled?: boolean;
    keepAliveInterval?: number;
  };
  httpFallback?: boolean;
  queryLogger?: (entry: QueryLogEntry) => void;
  idempotency?: ExtendedIdempotencyConfig;
}

/**
 * Query log entry for logging tests
 */
export interface QueryLogEntry {
  sql: string;
  startTime: number;
  endTime: number;
  duration: number;
}

/**
 * Eviction event for cache eviction callbacks
 */
export interface EvictionEvent {
  evictedKey: string;
  reason: 'lru' | 'ttl' | 'manual';
}

/**
 * Creates a test client with proper typing.
 */
export function createTestClient(config: TestClientConfig): DoSQLClient {
  // Build valid config with only defined properties (for exactOptionalPropertyTypes)
  const validConfig: SQLClientConfig = {
    url: config.url,
  };
  if (config.token !== undefined) validConfig.token = config.token;
  if (config.database !== undefined) validConfig.database = config.database;
  if (config.timeout !== undefined) validConfig.timeout = config.timeout;
  if (config.retry !== undefined) validConfig.retry = config.retry;
  if (config.idempotency !== undefined) {
    validConfig.idempotency = {
      enabled: config.idempotency.enabled,
    };
    if (config.idempotency.keyPrefix !== undefined) validConfig.idempotency.keyPrefix = config.idempotency.keyPrefix;
    if (config.idempotency.maxCacheSize !== undefined) validConfig.idempotency.maxCacheSize = config.idempotency.maxCacheSize;
    if (config.idempotency.cacheTtlMs !== undefined) validConfig.idempotency.cacheTtlMs = config.idempotency.cacheTtlMs;
    if (config.idempotency.cleanupIntervalMs !== undefined) validConfig.idempotency.cleanupIntervalMs = config.idempotency.cleanupIntervalMs;
    if (config.idempotency.ttlMs !== undefined) validConfig.idempotency.ttlMs = config.idempotency.ttlMs;
  }
  return new DoSQLClient(validConfig);
}

/**
 * Default test client configuration
 */
export const DEFAULT_TEST_CONFIG: TestClientConfig = {
  url: 'ws://localhost:8080',
};

// =============================================================================
// Extended Client Interface for GAP Testing
// =============================================================================

/**
 * Extended client interface that includes methods for testing GAP features.
 * Use type guards to check if methods exist before calling.
 */
export interface ExtendedClient extends SQLClient {
  // Connection management (potentially exists)
  reconnect?(): Promise<void>;
  getNetworkDiagnostics?(): Promise<NetworkDiagnostics>;
  getTimeoutConfig?(): TimeoutConfig;
  getConfig?(): SQLClientConfig;
  config?: SQLClientConfig;

  // Query extensions (potentially exists)
  executeRaw?(sql: string): Promise<QueryResult>;
  run?(sql: string): Promise<{ success: boolean }>;
  queryOne?<T>(sql: string, params?: SQLValue[]): Promise<T>;
  queryValue?<T>(sql: string, params?: SQLValue[]): Promise<T>;
  exists?(sql: string, params?: SQLValue[]): Promise<boolean>;
  explainQuery?(sql: string): Promise<{ plan: unknown }>;
  queryStreaming?(sql: string): Promise<AsyncIterable<unknown>>;

  // Batch extensions (potentially exists)
  executeMany?(
    sql: string,
    paramSets: SQLValue[][],
    options?: { transactional?: boolean }
  ): Promise<{
    totalRowsAffected: number;
    results: QueryResult[];
    transactionId?: string;
  }>;

  // Transaction extensions (potentially exists)
  savepoint?(txId: TransactionId, name: string): Promise<{ name: string }>;
  rollbackToSavepoint?(txId: TransactionId, name: string): Promise<void>;
  releaseSavepoint?(txId: TransactionId, name: string): Promise<void>;
  getTransactionState?(txId: TransactionId): Promise<TransactionStateInfo>;
  retryableTransaction?<T>(
    fn: (tx: TransactionContext) => Promise<T>,
    options?: { maxRetries: number }
  ): Promise<T>;

  // Pool extensions (potentially exists)
  hasPool?(): boolean;
  getPoolStats?(): PoolStats | null;
  getPoolHealth?(): PoolHealth;
  getConnectionInfo?(): ConnectionInfo[];
  getConnectionTags?(): string[];
  isHibernationEnabled?(): boolean;

  // Idempotency extensions (potentially exists)
  getIdempotencyCacheStatus?(key: string): CacheStatus;
  getPendingIdempotencyKeys?(): Map<string, string> | string[];
  getRetryCount?(key: string): number;
  isIdempotencyKeyExpired?(key: string): boolean;
  setIdempotencyKey?(sql: string, params: SQLValue[], key: string): void;
  detectHashCollision?(sql1: string, params1: SQLValue[], sql2: string, params2: SQLValue[]): boolean;
  getIdempotencyStats?(): IdempotencyStats;
  pinIdempotencyKey?(sql: string, params: SQLValue[]): void;
  unpinIdempotencyKey?(sql: string, params: SQLValue[]): void;
  isPinned?(sql: string, params: SQLValue[]): boolean;
  getCacheEntryMetadata?(sql: string, params: SQLValue[]): CacheEntryMetadata;
  getIdempotencyCacheConfig?(): IdempotencyCacheConfig;
  getCleanupStats?(): CleanupStats;
  getIdempotencyCacheMemoryUsage?(): number;
  estimateIdempotencyEntrySize?(sql: string, params: SQLValue[]): number;
  beginTransactionWithIdempotency?(): Promise<{ idempotencyKey: string }>;
  getTransactionIdempotencyKey?(txId: TransactionId): string;
  buildCommitParams?(txId: TransactionId, idempotencyKey: string): { idempotencyKey: string };
  getBatchIdempotencyKey?(statements: Array<{ sql: string; params: SQLValue[] }>): Promise<string>;
  generateBatchSubKeys?(batchKey: string, count: number): string[];
  handleIdempotencyExpired?(sql: string, params: SQLValue[], expiredKey: string): Promise<string>;
  execWithFullIdempotency?(sql: string, params: SQLValue[]): Promise<QueryResult & { idempotencyKey: string; fromIdempotencyCache: boolean; idempotencyKeyExpiresAt: number }>;
  getIdempotencyStatus?(): Promise<{ enabled: boolean; cachedKeyCount: number; ttlMs: number }>;
  getRPCHeaders?(method: string, payload: unknown): Record<string, string>;
  buildHttpRequest?(method: string, payload: unknown): { headers: Headers };

  // Stats (potentially exists)
  getStats?(): ClientStats;
  getReplicaStatus?(): ReplicaStatus;
}

/**
 * Network diagnostics information
 */
export interface NetworkDiagnostics {
  connected: boolean;
  latencyMs: number;
  reconnectAttempts: number;
  lastError: Error | null;
  connectionUptime: number;
}

/**
 * Timeout configuration
 */
export interface TimeoutConfig {
  connectionTimeout: number;
  requestTimeout: number;
}

/**
 * Transaction state information
 */
export interface TransactionStateInfo {
  id: TransactionId;
  status: 'active' | 'committed' | 'rolled_back';
  startedAt: Date;
  statementCount: number;
  isolationLevel?: string;
  readOnly?: boolean;
  snapshotLSN?: LSN;
}

/**
 * Pool health information
 */
export interface PoolHealth {
  healthy: boolean;
  healthyConnections: number;
  unhealthyConnections: number;
  lastHealthCheck: Date | null;
  averageLatency: number;
}

/**
 * Connection information
 */
export interface ConnectionInfo {
  id: string;
  createdAt: Date;
  lastUsedAt: Date;
  idleTime: number;
  active: boolean;
  tags: string[];
  latency: number | null;
}

/**
 * Cache status for idempotency key
 */
export interface CacheStatus {
  inCache: boolean;
  cachedAt: number;
}

/**
 * Idempotency statistics
 */
export interface IdempotencyStats {
  collisionCount: number;
  totalKeysGenerated: number;
}

/**
 * Cache entry metadata
 */
export interface CacheEntryMetadata {
  createdAt: number;
  lastAccessedAt: number;
  ttlMs?: number;
}

/**
 * Idempotency cache configuration
 */
export interface IdempotencyCacheConfig {
  cleanupIntervalMs: number;
}

/**
 * Cleanup statistics
 */
export interface CleanupStats {
  lastCleanupAt: number;
  entriesRemoved: number;
  totalCleanups: number;
}

/**
 * Client statistics
 */
export interface ClientStats {
  totalQueries: number;
  totalErrors: number;
  averageLatency: number;
  connectionUptime: number;
}

/**
 * Replica status
 */
export interface ReplicaStatus {
  primary: string;
  replicas: string[];
}

/**
 * Casts a client to the extended interface for testing GAP features.
 * This is a controlled type cast that provides proper types for optional methods.
 */
export function asExtendedClient(client: DoSQLClient | SQLClient): ExtendedClient {
  return client as ExtendedClient;
}

// =============================================================================
// Query Result Fixtures
// =============================================================================

/**
 * Creates a mock query result with proper typing.
 */
export function createQueryResult<T = Record<string, SQLValue>>(
  rows: T[] = [],
  options: Partial<Omit<QueryResult<T>, 'rows'>> = {}
): QueryResult<T> {
  const result: QueryResult<T> = {
    rows,
    columns: options.columns ?? [],
    rowsAffected: options.rowsAffected ?? 0,
    duration: options.duration ?? 0,
  };
  // Only add optional properties if defined (for exactOptionalPropertyTypes)
  if (options.lastInsertRowid !== undefined) result.lastInsertRowid = options.lastInsertRowid;
  if (options.columnTypes !== undefined) result.columnTypes = options.columnTypes;
  if (options.lsn !== undefined) result.lsn = options.lsn;
  if (options.hasMore !== undefined) result.hasMore = options.hasMore;
  if (options.cursor !== undefined) result.cursor = options.cursor;
  return result;
}

/**
 * Creates a mock transaction state with proper typing.
 */
export function createTransactionState(
  id?: string,
  options: Partial<Omit<TransactionState, 'id'>> = {}
): TransactionState {
  return {
    id: createTransactionId(id ?? `txn-${Date.now()}`),
    isolationLevel: options.isolationLevel ?? 'SERIALIZABLE',
    readOnly: options.readOnly ?? false,
    startedAt: options.startedAt ?? new Date(),
    snapshotLSN: options.snapshotLSN ?? createLSN(BigInt(0)),
  };
}

// =============================================================================
// Extended Error Types
// =============================================================================

/**
 * SQL Error with extended properties for testing GAP features
 */
export interface ExtendedSQLError extends Error {
  code: string;
  retryable?: boolean;
  recoverySuggestion?: string;
  partialResults?: QueryResult[];
  failedIndex?: number;
  request?: {
    sql: string;
    method: string;
  };
  idempotencyKey?: string;
}

/**
 * Creates an extended SQL error for testing
 */
export function createExtendedError(
  message: string,
  options: Partial<ExtendedSQLError> = {}
): ExtendedSQLError {
  const error = new Error(message) as ExtendedSQLError;
  error.code = options.code ?? 'UNKNOWN_ERROR';
  if (options.retryable !== undefined) error.retryable = options.retryable;
  if (options.recoverySuggestion) error.recoverySuggestion = options.recoverySuggestion;
  if (options.partialResults) error.partialResults = options.partialResults;
  if (options.failedIndex !== undefined) error.failedIndex = options.failedIndex;
  if (options.request) error.request = options.request;
  if (options.idempotencyKey) error.idempotencyKey = options.idempotencyKey;
  return error;
}

// =============================================================================
// Event Testing Utilities
// =============================================================================

/**
 * Type-safe event listener for client events
 */
export function createEventListener<K extends keyof ClientEventMap>(
  eventName: K
): { listener: ClientEventListener<K>; calls: ClientEventMap[K][] } {
  const calls: ClientEventMap[K][] = [];
  const listener: ClientEventListener<K> = (event) => {
    calls.push(event);
  };
  return { listener, calls };
}

/**
 * Creates a mock connected event
 */
export function createConnectedEvent(url: string = 'ws://localhost:8080'): ConnectedEvent {
  return {
    url,
    timestamp: new Date(),
  };
}

/**
 * Creates a mock disconnected event
 */
export function createDisconnectedEvent(
  url: string = 'ws://localhost:8080',
  reason?: string
): DisconnectedEvent {
  const event: DisconnectedEvent = {
    url,
    timestamp: new Date(),
  };
  // Only add optional reason if defined (for exactOptionalPropertyTypes)
  if (reason !== undefined) event.reason = reason;
  return event;
}

/**
 * Creates a mock error event
 */
export function createErrorEvent(
  error: Error,
  context: ClientErrorEvent['context'] = 'rpc'
): ClientErrorEvent {
  return {
    error,
    timestamp: new Date(),
    context,
  };
}

// =============================================================================
// Extended Transaction Context
// =============================================================================

/**
 * Extended transaction context for testing GAP features
 */
export interface ExtendedTransactionContext extends TransactionContext {
  isolationLevel?: string;
  readOnly?: boolean;
  startedAt?: Date;
  snapshotLSN?: LSN;
  idempotencyKey?: string;
}

/**
 * Type guard to check if a transaction context has extended properties
 */
export function hasExtendedTxProperties(
  tx: TransactionContext
): tx is ExtendedTransactionContext {
  const extended = tx as ExtendedTransactionContext;
  return (
    'isolationLevel' in extended ||
    'readOnly' in extended ||
    'startedAt' in extended ||
    'snapshotLSN' in extended ||
    'idempotencyKey' in extended
  );
}

// =============================================================================
// Assertion Helpers
// =============================================================================

/**
 * Helper to safely access potentially undefined methods
 */
export function methodExists<T extends object, K extends string>(
  obj: T,
  method: K
): obj is T & Record<K, (...args: unknown[]) => unknown> {
  return method in obj && typeof (obj as Record<string, unknown>)[method] === 'function';
}

/**
 * Helper to safely access potentially undefined properties
 */
export function propertyExists<T extends object, K extends string>(
  obj: T,
  prop: K
): obj is T & Record<K, unknown> {
  return prop in obj;
}

// =============================================================================
// Response Types for WebSocket Messages
// =============================================================================

/**
 * WebSocket response message types
 */
export interface WSResponseMessage {
  type: string;
  id?: string;
  result?: QueryResult;
  error?: {
    code: string;
    message: string;
  };
}

/**
 * Rate limiting response
 */
export interface RateLimitResponse extends WSResponseMessage {
  type: 'nack';
  reason: 'rate_limited' | 'memory_pressure';
}

/**
 * Acknowledgment response
 */
export interface AckResponse extends WSResponseMessage {
  type: 'ack';
}

/**
 * Type guard for rate limit response
 */
export function isRateLimitResponse(response: WSResponseMessage): response is RateLimitResponse {
  return response.type === 'nack' &&
    (response as RateLimitResponse).reason === 'rate_limited';
}

/**
 * Type guard for ack response
 */
export function isAckResponse(response: WSResponseMessage): response is AckResponse {
  return response.type === 'ack';
}

/**
 * Filters responses by type with proper typing
 */
export function filterResponses<T extends WSResponseMessage>(
  responses: WSResponseMessage[],
  predicate: (r: WSResponseMessage) => r is T
): T[] {
  return responses.filter(predicate);
}

// =============================================================================
// Reconnection Event Types
// =============================================================================

/**
 * Reconnecting event for tracking reconnection attempts
 */
export interface ReconnectingEvent {
  attempt: number;
  delayMs: number;
  maxAttempts: number;
}

/**
 * Reconnection event callback type
 */
export type ReconnectingEventCallback = (event: ReconnectingEvent) => void;

/**
 * Creates a reconnection event collector
 */
export function createReconnectionCollector(): {
  delays: number[];
  events: ReconnectingEvent[];
  callback: ReconnectingEventCallback;
} {
  const delays: number[] = [];
  const events: ReconnectingEvent[] = [];
  const callback: ReconnectingEventCallback = (e) => {
    delays.push(e.delayMs);
    events.push(e);
  };
  return { delays, events, callback };
}

// =============================================================================
// Query Logger Types
// =============================================================================

/**
 * Query log entry type
 */
export interface QueryLogEntry {
  sql: string;
  startTime: number;
  endTime: number;
  duration: number;
  params?: SQLValue[];
  error?: Error;
}

/**
 * Creates a query log collector
 */
export function createQueryLogCollector(): {
  log: QueryLogEntry[];
  logger: (entry: QueryLogEntry) => void;
} {
  const log: QueryLogEntry[] = [];
  const logger = (entry: QueryLogEntry) => log.push(entry);
  return { log, logger };
}

// =============================================================================
// Transaction Context Extension Types
// =============================================================================

/**
 * Transaction closure type with proper typing
 */
export type TransactionClosure<T> = (tx: TransactionContext) => Promise<T>;

/**
 * Transaction closure with idempotency key access
 */
export interface TransactionContextWithIdempotency extends TransactionContext {
  idempotencyKey?: string;
}

/**
 * Retryable transaction options
 */
export interface RetryableTransactionOptions {
  maxRetries?: number;
  retryDelayMs?: number;
  onRetry?: (attempt: number, error: Error) => void;
}

// =============================================================================
// WebSocket Event Types for Pool
// =============================================================================

/**
 * WebSocket event data union type
 */
export type WebSocketEventPayload =
  | { type: 'open' }
  | { type: 'close'; code?: number; reason?: string }
  | { type: 'message'; data: string }
  | { type: 'error'; error: Error };

/**
 * Type-safe WebSocket event callback
 */
export type TypedWebSocketCallback = (event: WebSocketEventPayload) => void;

/**
 * Mock WebSocket with typed event handling
 */
export interface TypedMockWebSocket {
  id: string;
  url: string;
  readyState: number;
  createdAt: number;
  lastUsedAt: number;

  addEventListener(event: string, callback: TypedWebSocketCallback): void;
  removeEventListener(event: string, callback: TypedWebSocketCallback): void;
  send(data: string): void;
  close(): void;

  // Test helpers
  simulateOpen(): void;
  simulateClose(code?: number, reason?: string): void;
  simulateMessage(data: unknown): void;
  simulateError(error: Error): void;
}

/**
 * Creates a typed mock WebSocket for connection pool tests
 */
export function createTypedMockWebSocket(url: string): TypedMockWebSocket {
  const listeners = new Map<string, Set<TypedWebSocketCallback>>();
  let readyState = 0;

  const emit = (event: string, payload: WebSocketEventPayload) => {
    listeners.get(event)?.forEach((cb) => cb(payload));
  };

  const ws: TypedMockWebSocket = {
    id: `ws-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    url,
    get readyState() { return readyState; },
    createdAt: Date.now(),
    lastUsedAt: Date.now(),

    addEventListener(event, callback) {
      if (!listeners.has(event)) {
        listeners.set(event, new Set());
      }
      listeners.get(event)!.add(callback);
    },

    removeEventListener(event, callback) {
      listeners.get(event)?.delete(callback);
    },

    send(data: string) {
      ws.lastUsedAt = Date.now();
      const request = JSON.parse(data) as { id: string };
      setTimeout(() => {
        emit('message', {
          type: 'message',
          data: JSON.stringify({
            id: request.id,
            result: { rows: [], rowsAffected: 0 },
          }),
        });
      }, 5);
    },

    close() {
      readyState = 3;
      emit('close', { type: 'close' });
    },

    // Test helpers
    simulateOpen() {
      readyState = 1;
      emit('open', { type: 'open' });
    },

    simulateClose(code?: number, reason?: string) {
      readyState = 3;
      const closeEvent: { type: 'close'; code?: number; reason?: string } = { type: 'close' };
      // Only add optional properties if defined (for exactOptionalPropertyTypes)
      if (code !== undefined) closeEvent.code = code;
      if (reason !== undefined) closeEvent.reason = reason;
      emit('close', closeEvent);
    },

    simulateMessage(data: unknown) {
      emit('message', { type: 'message', data: JSON.stringify(data) });
    },

    simulateError(error: Error) {
      emit('error', { type: 'error', error });
    },
  };

  // Simulate async connection
  setTimeout(() => {
    readyState = 1;
    emit('open', { type: 'open' });
  }, 10);

  return ws;
}
