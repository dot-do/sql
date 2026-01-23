/**
 * CDC (Change Data Capture) Types for DoSQL
 *
 * Types for streaming changes from the WAL to subscribers.
 *
 * This module re-exports unified CDC types from @dotdo/shared-types via @dotdo/sql.do
 * and provides CDC-specific types for the server implementation.
 */

import type { WALEntry, WALOperation, WALReader } from '../wal/types.js';

// =============================================================================
// Re-export Unified CDC Types from shared-types via sql.do
// =============================================================================

/**
 * Re-exported CDC types from `@dotdo/shared-types` (via `@dotdo/sql.do`).
 *
 * These types provide the canonical Change Data Capture definitions for the
 * DoSQL ecosystem. Using these shared types ensures compatibility between
 * CDC producers and consumers across the stack.
 *
 * ## Type Re-exports
 *
 * - {@link CDCOperation} - Union type of CDC operations: 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE'
 *   TRUNCATE is only used server-side; clients receive INSERT/UPDATE/DELETE only.
 *
 * - `UnifiedCDCEvent` (aliased from CDCEvent) - The canonical CDC event structure.
 *   Contains LSN, table name, operation, timestamp, and before/after row data.
 *   Note: This module also defines a local `CDCEvent` type that extends the unified
 *   type with transaction boundary events for server-side use.
 *
 * ## Value Re-exports
 *
 * - `CDCOperationCode` - Numeric codes for efficient binary encoding:
 *   - INSERT: 0
 *   - UPDATE: 1
 *   - DELETE: 2
 *   - TRUNCATE: 3
 *
 * ## Type Guards
 *
 * Functions for detecting CDC event format:
 *
 * - `isServerCDCEvent(event)` - Returns true if event has `txId` field (server format)
 * - `isClientCDCEvent(event)` - Returns true if event has `transactionId` field (client format)
 * - `isDateTimestamp(timestamp)` - Returns true if timestamp is a Date object
 * - `isNumericTimestamp(timestamp)` - Returns true if timestamp is a Unix timestamp number
 *
 * ## Type Converters
 *
 * Functions for converting between server and client CDC event formats:
 *
 * - `serverToClientCDCEvent(event)` - Converts server format to client format:
 *   - Converts numeric timestamps to Date objects
 *   - Normalizes `oldRow`/`newRow` to `before`/`after`
 *   - Normalizes `txId` to `transactionId`
 *
 * - `clientToServerCDCEvent(event)` - Converts client format to server format:
 *   - Converts Date timestamps to Unix timestamps
 *   - Normalizes `before`/`after` to `oldRow`/`newRow`
 *   - Normalizes `transactionId` to `txId`
 *
 * @example
 * ```typescript
 * import {
 *   CDCOperation,
 *   UnifiedCDCEvent,
 *   CDCOperationCode,
 *   serverToClientCDCEvent,
 * } from './types.js';
 *
 * // Process a CDC event from the WAL
 * function processCDCEvent(event: UnifiedCDCEvent): void {
 *   const clientEvent = serverToClientCDCEvent(event);
 *
 *   switch (clientEvent.operation) {
 *     case 'INSERT':
 *       console.log('New row:', clientEvent.after);
 *       break;
 *     case 'UPDATE':
 *       console.log('Updated:', clientEvent.before, '->', clientEvent.after);
 *       break;
 *     case 'DELETE':
 *       console.log('Deleted:', clientEvent.before);
 *       break;
 *   }
 * }
 * ```
 *
 * @see {@link https://github.com/dotdo/shared-types | @dotdo/shared-types} for canonical definitions
 *
 * @public
 * @stability experimental
 * @since 0.1.0
 */
export {
  type CDCOperation,
  type CDCEvent as UnifiedCDCEvent,
  CDCOperationCode,
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  serverToClientCDCEvent,
  clientToServerCDCEvent,
} from '@dotdo/sql.do';

// =============================================================================
// CDC Filter Types
// =============================================================================

/**
 * Filter for CDC subscriptions
 */
export interface CDCFilter {
  /** Filter by table names */
  tables?: string[];
  /** Filter by operation types */
  operations?: WALOperation[];
  /** Filter by transaction IDs */
  txnIds?: string[];
  /** Custom predicate function */
  predicate?: (entry: WALEntry) => boolean;
}

/**
 * Options for CDC subscription
 */
export interface CDCSubscriptionOptions {
  /** Starting LSN (exclusive - will receive entries after this LSN) */
  fromLSN?: bigint;
  /** Filter criteria */
  filter?: CDCFilter;
  /** Poll interval in milliseconds for checking new entries (default: 100) */
  pollInterval?: number;
  /** Batch size for reading entries (default: 100) */
  batchSize?: number;
  /** Maximum entries to buffer before applying backpressure (default: 1000) */
  maxBufferSize?: number;
  /** Whether to include transaction control entries (BEGIN/COMMIT/ROLLBACK) */
  includeTransactionControl?: boolean;
}

// =============================================================================
// CDC Event Types (module-specific, compatible with shared types)
// =============================================================================

/**
 * Structured change event for easier consumption
 */
export interface ChangeEvent<T = unknown> {
  /** Unique event ID (based on LSN) */
  id: string;
  /** Type of change */
  type: 'insert' | 'update' | 'delete';
  /** Table name */
  table: string;
  /** Transaction ID */
  txnId: string;
  /** Event timestamp */
  timestamp: Date;
  /** LSN of the change */
  lsn: bigint;
  /** The new value (for insert/update) */
  data?: T;
  /** The previous value (for update/delete) */
  oldData?: T;
  /** Primary key (if available) */
  key?: Uint8Array;
}

/**
 * Transaction boundary event
 */
export interface TransactionEvent {
  /** Event type */
  type: 'begin' | 'commit' | 'rollback';
  /** Transaction ID */
  txnId: string;
  /** Event timestamp */
  timestamp: Date;
  /** LSN of the event */
  lsn: bigint;
}

/**
 * Union type for all CDC events (module-specific)
 *
 * Note: This is compatible with the unified CDCEvent from shared-types
 * but includes transaction boundary events specific to the server.
 */
export type CDCEvent<T = unknown> = ChangeEvent<T> | TransactionEvent;

// =============================================================================
// CDC Subscription Interface
// =============================================================================

/**
 * Status of a CDC subscription
 */
export interface SubscriptionStatus {
  /** Whether the subscription is active */
  active: boolean;
  /** Current position (last processed LSN) */
  currentLSN: bigint;
  /** Number of entries processed */
  entriesProcessed: number;
  /** Number of entries pending in buffer */
  bufferedEntries: number;
  /** Subscription start time */
  startedAt: Date;
  /** Last entry received time */
  lastEntryAt?: Date;
}

/**
 * CDC Subscription interface
 */
export interface CDCSubscription {
  /**
   * Get async iterator over WAL entries
   */
  subscribe(
    fromLSN: bigint,
    filter?: CDCFilter
  ): AsyncIterableIterator<WALEntry>;

  /**
   * Get async iterator over structured change events
   */
  subscribeChanges<T = unknown>(
    fromLSN: bigint,
    filter?: CDCFilter,
    decoder?: (data: Uint8Array) => T
  ): AsyncIterableIterator<CDCEvent<T>>;

  /**
   * Get current subscription status
   */
  getStatus(): SubscriptionStatus;

  /**
   * Stop the subscription
   */
  stop(): void;

  /**
   * Check if subscription is active
   */
  isActive(): boolean;
}

// =============================================================================
// CDC Stream Types
// =============================================================================

/**
 * Callback-based CDC handler
 */
export interface CDCHandler<T = unknown> {
  /** Called for each change event */
  onChange?: (event: ChangeEvent<T>) => Promise<void> | void;
  /** Called for transaction boundaries */
  onTransaction?: (event: TransactionEvent) => Promise<void> | void;
  /** Called when an error occurs */
  onError?: (error: Error) => void;
  /** Called when stream ends or is stopped */
  onEnd?: () => void;
}

/**
 * Options for CDC stream
 */
export interface CDCStreamOptions extends CDCSubscriptionOptions {
  /** Handler callbacks */
  handler: CDCHandler;
  /** Decoder for converting Uint8Array to typed data */
  decoder?: (data: Uint8Array) => unknown;
  /** Whether to auto-acknowledge (advance position) after each entry */
  autoAck?: boolean;
}

/**
 * CDC Stream interface (callback-based alternative to iterator)
 */
export interface CDCStream {
  /** Start the stream */
  start(): void;
  /** Stop the stream */
  stop(): void;
  /** Pause the stream (buffering continues) */
  pause(): void;
  /** Resume the stream */
  resume(): void;
  /** Get current status */
  getStatus(): SubscriptionStatus;
  /** Manually acknowledge processing up to LSN */
  acknowledge(lsn: bigint): void;
}

// =============================================================================
// Replication Slot Types
// =============================================================================

/**
 * Replication slot - persistent position tracking for CDC consumers
 */
export interface ReplicationSlot {
  /** Unique slot name */
  name: string;
  /** Last acknowledged LSN */
  acknowledgedLSN: bigint;
  /** When the slot was created */
  createdAt: Date;
  /** When the slot was last used */
  lastUsedAt: Date;
  /** Associated filter (optional) */
  filter?: CDCFilter;
  /** Custom metadata */
  metadata?: Record<string, string>;
}

/**
 * Replication slot manager interface
 */
export interface ReplicationSlotManager {
  /** Create a new replication slot */
  createSlot(
    name: string,
    initialLSN?: bigint,
    filter?: CDCFilter
  ): Promise<ReplicationSlot>;

  /** Get a replication slot by name */
  getSlot(name: string): Promise<ReplicationSlot | null>;

  /** Update slot position */
  updateSlot(name: string, acknowledgedLSN: bigint): Promise<void>;

  /** Delete a replication slot */
  deleteSlot(name: string): Promise<void>;

  /** List all replication slots */
  listSlots(): Promise<ReplicationSlot[]>;

  /** Create a subscription from a slot */
  subscribeFromSlot(name: string): Promise<CDCSubscription>;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * CDC-specific error codes
 */
export enum CDCErrorCode {
  /** Subscription failed to start */
  SUBSCRIPTION_FAILED = 'CDC_SUBSCRIPTION_FAILED',
  /** LSN not found (too old, already compacted) */
  LSN_NOT_FOUND = 'CDC_LSN_NOT_FOUND',
  /** Replication slot not found */
  SLOT_NOT_FOUND = 'CDC_SLOT_NOT_FOUND',
  /** Replication slot already exists */
  SLOT_EXISTS = 'CDC_SLOT_EXISTS',
  /** Buffer overflow */
  BUFFER_OVERFLOW = 'CDC_BUFFER_OVERFLOW',
  /** Decoder error */
  DECODE_ERROR = 'CDC_DECODE_ERROR',
}

/**
 * Custom error class for CDC operations
 */
export class CDCError extends Error {
  constructor(
    public readonly code: CDCErrorCode,
    message: string,
    public readonly lsn?: bigint,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'CDCError';
  }
}

// =============================================================================
// Lakehouse Streaming Types
// =============================================================================

/**
 * Configuration for streaming CDC to lakehouse
 */
export interface LakehouseStreamConfig {
  /** Target lakehouse WebSocket URL */
  lakehouseUrl: string;
  /** Source DO identifier */
  sourceDoId: string;
  /** Source shard name (optional) */
  sourceShardName?: string;
  /** Maximum batch size for transfer */
  maxBatchSize: number;
  /** Maximum batch age before forced flush (ms) */
  maxBatchAge: number;
  /** Retry configuration */
  retry: RetryConfig;
  /** Heartbeat interval (ms) */
  heartbeatInterval: number;
  /** Enable exactly-once semantics */
  exactlyOnce: boolean;
}

/**
 * Default lakehouse stream configuration
 */
export const DEFAULT_LAKEHOUSE_CONFIG: LakehouseStreamConfig = {
  lakehouseUrl: '',
  sourceDoId: '',
  maxBatchSize: 1000,
  maxBatchAge: 5000, // 5 seconds
  retry: {
    maxAttempts: 3,
    initialDelayMs: 100,
    maxDelayMs: 10000,
    backoffMultiplier: 2,
  },
  heartbeatInterval: 30000, // 30 seconds
  exactlyOnce: true,
};

/**
 * Retry configuration
 */
export interface RetryConfig {
  /** Maximum retry attempts */
  maxAttempts: number;
  /** Initial retry delay (ms) */
  initialDelayMs: number;
  /** Maximum retry delay (ms) */
  maxDelayMs: number;
  /** Exponential backoff multiplier */
  backoffMultiplier: number;
}

/**
 * Lakehouse acknowledgment
 */
export interface LakehouseAck {
  /** Acknowledged LSN */
  lsn: bigint;
  /** Acknowledgment status */
  status: 'ok' | 'buffered' | 'persisted' | 'duplicate';
  /** Batch ID that was acknowledged */
  batchId?: string;
  /** Timestamp of acknowledgment */
  timestamp: number;
}

/**
 * Lakehouse negative acknowledgment
 */
export interface LakehouseNack {
  /** Sequence that was rejected */
  sequence: number;
  /** Rejection reason */
  reason: 'buffer_full' | 'rate_limited' | 'invalid_sequence' | 'internal_error';
  /** Should retry */
  shouldRetry: boolean;
  /** Suggested retry delay (ms) */
  retryDelayMs?: number;
  /** Error message */
  message: string;
}

/**
 * Status of lakehouse streaming connection
 */
export interface LakehouseStreamStatus {
  /** Connection state */
  state: 'disconnected' | 'connecting' | 'connected' | 'reconnecting' | 'error';
  /** Last acknowledged LSN */
  lastAckLSN: bigint;
  /** Last sent LSN */
  lastSentLSN: bigint;
  /** Pending batches count */
  pendingBatches: number;
  /** Total batches sent */
  totalBatchesSent: number;
  /** Total entries sent */
  totalEntriesSent: number;
  /** Last error (if any) */
  lastError?: string;
  /** Connected since timestamp */
  connectedSince?: number;
  /** Last heartbeat timestamp */
  lastHeartbeat?: number;
}

/**
 * CDC batch for lakehouse transfer
 */
export interface CDCBatch {
  /** Unique batch ID for deduplication */
  batchId: string;
  /** Source DO identifier */
  sourceDoId: string;
  /** Sequence number for ordering */
  sequenceNumber: number;
  /** First LSN in batch */
  firstLSN: bigint;
  /** Last LSN in batch */
  lastLSN: bigint;
  /** Change events in this batch */
  events: CDCEvent[];
  /** Batch creation timestamp */
  createdAt: number;
  /** Estimated size in bytes */
  sizeBytes: number;
  /** Whether this is a retry */
  isRetry: boolean;
  /** Retry count */
  retryCount: number;
}

/**
 * Schema change event for evolution tracking
 */
export interface SchemaChangeEvent {
  /** Event type */
  type: 'schema_change';
  /** Table affected */
  table: string;
  /** Change type */
  changeType: 'add_column' | 'drop_column' | 'alter_column' | 'create_table' | 'drop_table';
  /** Column name (for column changes) */
  column?: string;
  /** Old type (for alter) */
  oldType?: string;
  /** New type (for alter/add) */
  newType?: string;
  /** Schema version after change */
  schemaVersion: number;
  /** Timestamp */
  timestamp: number;
  /** LSN of schema change */
  lsn: bigint;
}

/**
 * Checkpoint for exactly-once delivery
 */
export interface DeliveryCheckpoint {
  /** Source DO ID */
  sourceDoId: string;
  /** Last committed LSN */
  committedLSN: bigint;
  /** Last committed batch ID */
  committedBatchId: string;
  /** Checkpoint timestamp */
  checkpointedAt: number;
  /** Pending batch IDs (in-flight) */
  pendingBatchIds: string[];
}

/**
 * Backpressure signal from lakehouse
 */
export interface BackpressureSignal {
  /** Type of backpressure */
  type: 'pause' | 'slow_down' | 'resume';
  /** Buffer utilization (0-1) */
  bufferUtilization: number;
  /** Suggested delay between batches (ms) */
  suggestedDelayMs?: number;
  /** Reason for backpressure */
  reason?: string;
}
