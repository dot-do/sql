/**
 * CDC (Change Data Capture) Types for DoSQL
 *
 * Types for streaming changes from the WAL to subscribers.
 *
 * This module re-exports unified CDC types from @dotdo/sql-types via sql.do
 * and provides CDC-specific types for the server implementation.
 */

import type { WALEntry, WALOperation, WALReader } from '../wal/types.js';
import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from '../errors/base.js';

// =============================================================================
// Re-export Unified CDC Types from shared-types via sql.do
// =============================================================================

/**
 * Re-exported CDC types from `@dotdo/sql-types` (via `sql.do`).
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
 * @see {@link https://github.com/dotdo/shared-types | @dotdo/sql-types} for canonical definitions
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
} from 'sql.do';

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
  data?: T | undefined;
  /** The previous value (for update/delete) */
  oldData?: T | undefined;
  /** Primary key (if available) */
  key?: Uint8Array | undefined;
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
// CDC OPERATION-BASED TYPE INFERENCE
// =============================================================================

/**
 * CDC operation types
 */
export type CDCOperationType = 'insert' | 'update' | 'delete';

/**
 * Transaction event types
 */
export type TransactionEventType = 'begin' | 'commit' | 'rollback';

/**
 * Insert event - has data (after), no oldData
 */
export interface InsertChangeEvent<T = unknown> extends Omit<ChangeEvent<T>, 'type' | 'oldData'> {
  type: 'insert';
  data: T;
}

/**
 * Update event - has both data (after) and oldData (before)
 */
export interface UpdateChangeEvent<T = unknown> extends Omit<ChangeEvent<T>, 'type'> {
  type: 'update';
  data: T;
  oldData: T;
}

/**
 * Delete event - has oldData (before), no data
 */
export interface DeleteChangeEvent<T = unknown> extends Omit<ChangeEvent<T>, 'type' | 'data'> {
  type: 'delete';
  oldData: T;
}

/**
 * Get the typed change event based on operation type
 *
 * @example
 * ```typescript
 * type InsertEvt = TypedChangeEvent<'insert', User>; // InsertChangeEvent<User>
 * type UpdateEvt = TypedChangeEvent<'update', User>; // UpdateChangeEvent<User>
 * type DeleteEvt = TypedChangeEvent<'delete', User>; // DeleteChangeEvent<User>
 * ```
 */
export type TypedChangeEvent<Op extends CDCOperationType, T = unknown> =
  Op extends 'insert' ? InsertChangeEvent<T> :
  Op extends 'update' ? UpdateChangeEvent<T> :
  Op extends 'delete' ? DeleteChangeEvent<T> :
  never;

/**
 * Extract the 'data' field type based on operation
 *
 * @example
 * ```typescript
 * type D1 = CDCEventData<'insert', User>; // User (required)
 * type D2 = CDCEventData<'update', User>; // User (required)
 * type D3 = CDCEventData<'delete', User>; // undefined (no data on delete)
 * ```
 */
export type CDCEventData<Op extends CDCOperationType, T = unknown> =
  Op extends 'insert' | 'update' ? T :
  Op extends 'delete' ? undefined :
  never;

/**
 * Extract the 'oldData' field type based on operation
 *
 * @example
 * ```typescript
 * type O1 = CDCEventOldData<'insert', User>; // undefined (no oldData on insert)
 * type O2 = CDCEventOldData<'update', User>; // User (required)
 * type O3 = CDCEventOldData<'delete', User>; // User (required)
 * ```
 */
export type CDCEventOldData<Op extends CDCOperationType, T = unknown> =
  Op extends 'insert' ? undefined :
  Op extends 'update' | 'delete' ? T :
  never;

/**
 * Type guard to check if a CDC operation has 'data' field
 */
export type HasData<Op extends CDCOperationType> =
  Op extends 'insert' | 'update' ? true : false;

/**
 * Type guard to check if a CDC operation has 'oldData' field
 */
export type HasOldData<Op extends CDCOperationType> =
  Op extends 'update' | 'delete' ? true : false;

/**
 * Type guard to narrow CDC event by operation type
 */
export function isInsertEvent<T>(event: ChangeEvent<T>): event is InsertChangeEvent<T> {
  return event.type === 'insert';
}

/**
 * Type guard to narrow CDC event by operation type
 */
export function isUpdateEvent<T>(event: ChangeEvent<T>): event is UpdateChangeEvent<T> {
  return event.type === 'update';
}

/**
 * Type guard to narrow CDC event by operation type
 */
export function isDeleteEvent<T>(event: ChangeEvent<T>): event is DeleteChangeEvent<T> {
  return event.type === 'delete';
}

/**
 * Type guard to check if event is a transaction event
 */
export function isTransactionEvent(event: CDCEvent): event is TransactionEvent {
  return event.type === 'begin' || event.type === 'commit' || event.type === 'rollback';
}

/**
 * Type guard to check if event is a change event
 */
export function isChangeEvent<T>(event: CDCEvent<T>): event is ChangeEvent<T> {
  return event.type === 'insert' || event.type === 'update' || event.type === 'delete';
}

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
  lastEntryAt?: Date | undefined;
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
  filter?: CDCFilter | undefined;
  /** Custom metadata */
  metadata?: Record<string, string> | undefined;
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
  /** Consumer pool exhausted */
  POOL_EXHAUSTED = 'CDC_POOL_EXHAUSTED',
  /** Consumer pool timeout */
  POOL_TIMEOUT = 'CDC_POOL_TIMEOUT',
  /** Consumer not found */
  CONSUMER_NOT_FOUND = 'CDC_CONSUMER_NOT_FOUND',
  /** Backpressure limit exceeded */
  BACKPRESSURE_LIMIT = 'CDC_BACKPRESSURE_LIMIT',
}

/**
 * Error class for CDC (Change Data Capture) operations
 *
 * Extends DoSQLError to provide consistent error handling with:
 * - Machine-readable error codes
 * - Error categories for API layer handling
 * - Recovery hints for developers
 * - LSN context for debugging streaming issues
 *
 * @example
 * ```typescript
 * try {
 *   const subscription = await cdcManager.subscribe({ fromLSN: 1000n });
 * } catch (error) {
 *   if (error instanceof CDCError) {
 *     if (error.code === CDCErrorCode.LSN_NOT_FOUND) {
 *       // LSN too old, need to perform full sync
 *       console.log('LSN not found:', error.lsn);
 *     }
 *     if (error.isRetryable()) {
 *       // Retry the subscription
 *     }
 *   }
 * }
 * ```
 */
export class CDCError extends DoSQLError {
  readonly code: CDCErrorCode;
  readonly category: ErrorCategory;
  readonly lsn?: bigint;

  constructor(
    code: CDCErrorCode,
    message: string,
    options?: {
      cause?: Error;
      context?: ErrorContext;
      lsn?: bigint;
    }
  ) {
    super(message, {
      ...(options?.cause !== undefined ? { cause: options.cause } : {}),
      ...(options?.context !== undefined ? { context: options.context } : {}),
    });
    this.name = 'CDCError';
    this.code = code;
    this.lsn = options?.lsn;

    // Set category based on error code
    this.category = this.determineCategory();

    // Include LSN in context
    if (this.lsn !== undefined) {
      this.context = {
        ...this.context,
        metadata: {
          ...this.context?.metadata,
          lsn: String(this.lsn),
        },
      };
    }

    // Set recovery hints
    this.setRecoveryHint();
  }

  private determineCategory(): ErrorCategory {
    switch (this.code) {
      case CDCErrorCode.LSN_NOT_FOUND:
      case CDCErrorCode.SLOT_NOT_FOUND:
      case CDCErrorCode.CONSUMER_NOT_FOUND:
        return ErrorCategory.RESOURCE;
      case CDCErrorCode.SLOT_EXISTS:
        return ErrorCategory.CONFLICT;
      case CDCErrorCode.BUFFER_OVERFLOW:
      case CDCErrorCode.POOL_EXHAUSTED:
      case CDCErrorCode.BACKPRESSURE_LIMIT:
        return ErrorCategory.RESOURCE;
      case CDCErrorCode.DECODE_ERROR:
        return ErrorCategory.VALIDATION;
      case CDCErrorCode.SUBSCRIPTION_FAILED:
        return ErrorCategory.CONNECTION;
      case CDCErrorCode.POOL_TIMEOUT:
        return ErrorCategory.TIMEOUT;
      default:
        return ErrorCategory.EXECUTION;
    }
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case CDCErrorCode.SUBSCRIPTION_FAILED:
        this.recoveryHint = 'Verify CDC is enabled and WAL is available. Retry with exponential backoff.';
        break;
      case CDCErrorCode.LSN_NOT_FOUND:
        this.recoveryHint = 'The requested LSN has been compacted. Start from the oldest available LSN or perform a full table sync.';
        break;
      case CDCErrorCode.SLOT_NOT_FOUND:
        this.recoveryHint = 'Create the replication slot before subscribing. Use createSlot() to create it.';
        break;
      case CDCErrorCode.SLOT_EXISTS:
        this.recoveryHint = 'Use a different slot name or delete the existing slot if it is no longer needed.';
        break;
      case CDCErrorCode.BUFFER_OVERFLOW:
        this.recoveryHint = 'Consumer is not keeping up. Increase buffer size or speed up event processing.';
        break;
      case CDCErrorCode.DECODE_ERROR:
        this.recoveryHint = 'Check event format and decoder compatibility. Verify schema matches expected format.';
        break;
      case CDCErrorCode.POOL_EXHAUSTED:
        this.recoveryHint = 'Consumer pool is at capacity. Wait for consumers to disconnect or increase pool size.';
        break;
      case CDCErrorCode.POOL_TIMEOUT:
        this.recoveryHint = 'Timed out waiting for a slot in the consumer pool. Retry later or reduce concurrent consumers.';
        break;
      case CDCErrorCode.CONSUMER_NOT_FOUND:
        this.recoveryHint = 'The specified consumer does not exist. It may have been disconnected or never registered.';
        break;
      case CDCErrorCode.BACKPRESSURE_LIMIT:
        this.recoveryHint = 'Too many consumers are experiencing backpressure. Slow down event production or scale consumers.';
        break;
    }
  }

  /**
   * Check if this error is retryable
   */
  isRetryable(): boolean {
    return [
      CDCErrorCode.SUBSCRIPTION_FAILED,
      CDCErrorCode.BUFFER_OVERFLOW,
      CDCErrorCode.POOL_TIMEOUT,
      CDCErrorCode.BACKPRESSURE_LIMIT,
    ].includes(this.code);
  }

  /**
   * Get a user-friendly error message
   */
  toUserMessage(): string {
    switch (this.code) {
      case CDCErrorCode.SUBSCRIPTION_FAILED:
        return 'Failed to start CDC subscription. Please try again.';
      case CDCErrorCode.LSN_NOT_FOUND:
        return 'The requested log position is no longer available. A full sync may be required.';
      case CDCErrorCode.SLOT_NOT_FOUND:
        return 'The replication slot does not exist.';
      case CDCErrorCode.SLOT_EXISTS:
        return 'A replication slot with this name already exists.';
      case CDCErrorCode.BUFFER_OVERFLOW:
        return 'CDC buffer is full. Please slow down or increase buffer capacity.';
      case CDCErrorCode.DECODE_ERROR:
        return 'Failed to decode CDC event data.';
      case CDCErrorCode.POOL_EXHAUSTED:
        return 'CDC consumer pool is at maximum capacity.';
      case CDCErrorCode.POOL_TIMEOUT:
        return 'Timed out waiting for a slot in the CDC consumer pool.';
      case CDCErrorCode.CONSUMER_NOT_FOUND:
        return 'The specified CDC consumer was not found.';
      case CDCErrorCode.BACKPRESSURE_LIMIT:
        return 'CDC backpressure limit exceeded. Please slow down.';
      default:
        return this.message;
    }
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): CDCError {
    const lsnStr = json.context?.metadata?.lsn as string | undefined;
    return new CDCError(
      json.code as CDCErrorCode,
      json.message,
      {
        ...(json.context !== undefined ? { context: json.context } : {}),
        ...(lsnStr !== undefined ? { lsn: BigInt(lsnStr) } : {}),
      }
    );
  }
}

// Register for deserialization
registerErrorClass('CDCError', CDCError);

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
  sourceShardName?: string | undefined;
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
export const DEFAULT_LAKEHOUSE_CONFIG: Readonly<LakehouseStreamConfig> = {
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
 *
 * Note: The full schema versioning types are available in 'dosql/schema':
 * - SchemaVersion (branded bigint type)
 * - SchemaChangeType (full set of change operations)
 * - SchemaChangeEvent (comprehensive event with compatibility level)
 * - SchemaVersionRegistry (for tracking schema evolution)
 * - SchemaAwareCDCProcessor (for enriching CDC events with schema metadata)
 *
 * This interface provides a simplified schema change event for CDC streams.
 * For full schema evolution support, use the schema versioning module.
 *
 * @see {@link import('../schema/versioning.js').SchemaChangeEvent} for full type
 */
export interface SchemaChangeEvent {
  /** Event type */
  type: 'schema_change';
  /** Table affected */
  table: string;
  /** Change type */
  changeType: 'add_column' | 'drop_column' | 'alter_column' | 'create_table' | 'drop_table' | 'rename_column' | 'rename_table';
  /** Column name (for column changes) */
  column?: string;
  /** Old column name (for renames) */
  oldColumnName?: string;
  /** Old type (for alter) */
  oldType?: string;
  /** New type (for alter/add) */
  newType?: string;
  /** Is column nullable */
  nullable?: boolean;
  /** Default value */
  defaultValue?: string;
  /** Schema version before change */
  beforeSchemaVersion?: number;
  /** Schema version after change */
  schemaVersion: number;
  /** Schema checksum for validation */
  schemaChecksum?: string;
  /** Compatibility level of this change */
  compatibility?: 'backward_compatible' | 'forward_compatible' | 'full_compatible' | 'breaking';
  /** Transaction ID */
  txnId?: string;
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
