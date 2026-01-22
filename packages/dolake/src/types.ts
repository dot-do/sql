/**
 * DoLake Types
 *
 * Types for the lakehouse component that receives CDC streams from DoSQL
 * and writes to R2 as Parquet/Iceberg.
 *
 * This module re-exports unified types from @dotdo/lake.do (which uses @dotdo/sql.do
 * and @dotdo/shared-types) and provides server-specific types.
 */

import {
  SIZE_LIMITS,
  TIMEOUTS,
  THRESHOLDS,
} from './constants.js';

// =============================================================================
// Re-export Unified Types from lake.do and sql.do
// =============================================================================

export {
  // CDC Types from shared-types via sql.do
  type CDCOperation,
  type CDCEvent,
  CDCOperationCode,
  type ClientCDCOperation,

  // Client Capabilities from shared-types via sql.do
  type ClientCapabilities,
  DEFAULT_CLIENT_CAPABILITIES,

  // Type Guards
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,

  // Type Converters
  serverToClientCDCEvent,
  clientToServerCDCEvent,
} from '@dotdo/lake.do';

// =============================================================================
// Timestamp Utilities
// =============================================================================

/**
 * Convert a Date | number timestamp to a numeric Unix timestamp (milliseconds).
 * Provides type narrowing for CDC event timestamps.
 */
export function getNumericTimestamp(timestamp: Date | number): number {
  if (timestamp instanceof Date) {
    return timestamp.getTime();
  }
  return timestamp;
}

// =============================================================================
// CDC Operation Codes (for efficient binary encoding)
// =============================================================================

// CDCOperationCode is re-exported from lake.do above
export type CDCOperationCodeValue = 0 | 1 | 2 | 3;

// =============================================================================
// RPC Message Types (DoSQL -> DoLake)
// =============================================================================

import type { CDCEvent, ClientCapabilities } from '@dotdo/lake.do';

/**
 * Message type discriminator
 */
export type RpcMessageType =
  | 'cdc_batch'
  | 'ack'
  | 'nack'
  | 'heartbeat'
  | 'connect'
  | 'disconnect'
  | 'flush_request'
  | 'status';

/**
 * Base interface for all RPC messages
 */
export interface RpcMessage {
  type: RpcMessageType;
  timestamp: number;
  correlationId?: string;
}

/**
 * CDC Batch message - DoSQL sends CDC events to DoLake
 */
export interface CDCBatchMessage extends RpcMessage {
  type: 'cdc_batch';

  /** ID of the source DoSQL Durable Object */
  sourceDoId: string;

  /** Human-readable name of the source shard */
  sourceShardName?: string;

  /** CDC events in this batch */
  events: CDCEvent[];

  /** Sequence number of this batch */
  sequenceNumber: number;

  /** First event sequence in this batch */
  firstEventSequence: number;

  /** Last event sequence in this batch */
  lastEventSequence: number;

  /** Total size of events in bytes (approximate) */
  sizeBytes: number;

  /** Whether this is a retry */
  isRetry: boolean;

  /** Retry count */
  retryCount: number;
}

/**
 * Connect message - DoSQL initiates connection to DoLake
 */
export interface ConnectMessage extends RpcMessage {
  type: 'connect';

  /** ID of the connecting DoSQL DO */
  sourceDoId: string;

  /** Shard name */
  sourceShardName?: string;

  /** Last acknowledged sequence number (for resumption) */
  lastAckSequence: number;

  /** Protocol version */
  protocolVersion: number;

  /** Client capabilities */
  capabilities: ClientCapabilities;
}

/**
 * Heartbeat message
 */
export interface HeartbeatMessage extends RpcMessage {
  type: 'heartbeat';

  /** ID of the source DO */
  sourceDoId: string;

  /** Last acknowledged sequence */
  lastAckSequence: number;

  /** Pending events count */
  pendingEvents: number;
}

/**
 * Flush request message
 */
export interface FlushRequestMessage extends RpcMessage {
  type: 'flush_request';

  /** ID of the requesting DO */
  sourceDoId: string;

  /** Reason for flush */
  reason: 'manual' | 'shutdown' | 'buffer_full' | 'time_threshold';
}

// =============================================================================
// RPC Message Types (DoLake -> DoSQL)
// =============================================================================

/**
 * Rate limit info included in responses
 */
export interface RateLimitInfo {
  /** Maximum allowed requests */
  limit: number;

  /** Remaining requests in window */
  remaining: number;

  /** Reset timestamp (Unix seconds) */
  resetAt: number;
}

/**
 * Acknowledgment message
 */
export interface AckMessage extends RpcMessage {
  type: 'ack';

  /** Sequence number being acknowledged */
  sequenceNumber: number;

  /** Status of the acknowledged batch */
  status: AckStatus;

  /** Batch ID */
  batchId?: string;

  /** Additional details */
  details?: AckDetails;

  /** Rate limit info */
  rateLimit?: RateLimitInfo;
}

/**
 * Acknowledgment status
 */
export type AckStatus =
  | 'ok'
  | 'buffered'
  | 'persisted'
  | 'duplicate'
  | 'fallback';

/**
 * Acknowledgment details
 */
export interface AckDetails {
  eventsProcessed: number;
  bufferUtilization: number;
  timeUntilFlush?: number;
  persistedPath?: string;
  /** Remaining tokens in rate limit bucket */
  remainingTokens?: number;
  /** Token bucket capacity */
  bucketCapacity?: number;
  /** Suggested delay for backpressure (ms) */
  suggestedDelayMs?: number;
  /** Circuit breaker state */
  circuitBreakerState?: 'closed' | 'open' | 'half-open';
}

/**
 * Negative acknowledgment
 */
export interface NackMessage extends RpcMessage {
  type: 'nack';

  /** Sequence number being rejected */
  sequenceNumber: number;

  /** Reason for rejection */
  reason: NackReason;

  /** Error message */
  errorMessage: string;

  /** Whether client should retry */
  shouldRetry: boolean;

  /** Suggested retry delay in ms */
  retryDelayMs?: number;

  /** Maximum allowed size (for size violations) */
  maxSize?: number;
}

/**
 * Reasons for negative acknowledgment
 */
export type NackReason =
  | 'buffer_full'
  | 'rate_limited'
  | 'invalid_sequence'
  | 'invalid_format'
  | 'internal_error'
  | 'shutting_down'
  | 'payload_too_large'
  | 'event_too_large'
  | 'load_shedding'
  | 'connection_limit'
  | 'ip_limit';

/**
 * Status response message
 */
export interface StatusMessage extends RpcMessage {
  type: 'status';

  /** Current DoLake state */
  state: DoLakeState;

  /** Buffer statistics */
  buffer: BufferStats;

  /** Connected sources */
  connectedSources: number;

  /** Last flush time */
  lastFlushTime?: number;

  /** Next scheduled flush */
  nextFlushTime?: number;
}

// =============================================================================
// Buffer Types
// =============================================================================

/**
 * A buffered batch in DoLake
 */
export interface BufferedBatch {
  /** Unique batch ID */
  batchId: string;

  /** Source DO ID */
  sourceDoId: string;

  /** Source shard name */
  sourceShardName?: string;

  /** CDC events in this batch */
  events: CDCEvent[];

  /** When batch was received */
  receivedAt: number;

  /** Sequence number */
  sequenceNumber: number;

  /** Whether persisted to R2 */
  persisted: boolean;

  /** Whether in fallback storage */
  inFallback: boolean;

  /** Size in bytes */
  sizeBytes: number;
}

/**
 * Buffer statistics
 */
export interface BufferStats {
  batchCount: number;
  eventCount: number;
  totalSizeBytes: number;
  utilization: number;
  oldestBatchTime?: number;
  newestBatchTime?: number;
}

/**
 * DoLake state
 */
export type DoLakeState =
  | 'idle'
  | 'receiving'
  | 'flushing'
  | 'recovering'
  | 'error';

// =============================================================================
// Flush Types
// =============================================================================

/**
 * Result of flushing buffers
 */
export interface FlushResult {
  success: boolean;
  batchesFlushed: number;
  eventsFlushed: number;
  bytesWritten: number;
  paths: string[];
  durationMs: number;
  error?: string;
  usedFallback: boolean;
}

/**
 * Flush trigger reasons
 */
export type FlushTrigger =
  | 'threshold_events'
  | 'threshold_size'
  | 'threshold_time'
  | 'manual'
  | 'shutdown'
  | 'memory_pressure';

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * DoLake configuration
 */
export interface DoLakeConfig {
  /** R2 bucket binding name */
  r2BucketName: string;

  /** Base path in R2 for Iceberg tables */
  r2BasePath: string;

  /** Maximum events before flush */
  flushThresholdEvents: number;

  /** Maximum size in bytes before flush */
  flushThresholdBytes: number;

  /** Maximum age of buffer before flush (ms) */
  flushThresholdMs: number;

  /** Interval for scheduled flushes (ms) */
  flushIntervalMs: number;

  /** Maximum buffer size in bytes */
  maxBufferSize: number;

  /** Enable local fallback storage */
  enableFallback: boolean;

  /** Maximum fallback storage size */
  maxFallbackSize: number;

  /** Enable deduplication */
  enableDeduplication: boolean;

  /** Deduplication window in ms */
  deduplicationWindowMs: number;

  /** Target Parquet row group size */
  parquetRowGroupSize: number;

  /** Enable Parquet compression */
  parquetCompression: 'none' | 'snappy' | 'gzip' | 'zstd';
}

/**
 * Default DoLake configuration
 */
export const DEFAULT_DOLAKE_CONFIG: DoLakeConfig = {
  r2BucketName: 'lakehouse-data',
  r2BasePath: 'warehouse',
  flushThresholdEvents: THRESHOLDS.FLUSH_THRESHOLD_EVENTS,
  flushThresholdBytes: SIZE_LIMITS.FLUSH_THRESHOLD_BYTES,
  flushThresholdMs: TIMEOUTS.FLUSH_THRESHOLD_MS,
  flushIntervalMs: TIMEOUTS.FLUSH_INTERVAL_MS,
  maxBufferSize: SIZE_LIMITS.MAX_BUFFER_SIZE,
  enableFallback: true,
  maxFallbackSize: SIZE_LIMITS.MAX_FALLBACK_SIZE,
  enableDeduplication: true,
  deduplicationWindowMs: TIMEOUTS.DEDUPLICATION_WINDOW_MS,
  parquetRowGroupSize: THRESHOLDS.PARQUET_ROW_GROUP_SIZE,
  parquetCompression: 'snappy',
};

// =============================================================================
// WebSocket Attachment Types
// =============================================================================

/**
 * Data stored with hibernating WebSocket connection
 */
export interface WebSocketAttachment {
  /** ID of the connected source DO */
  sourceDoId: string;

  /** Name of the source shard */
  sourceShardName?: string;

  /** Last acknowledged sequence number */
  lastAckSequence: number;

  /** Connection timestamp */
  connectedAt: number;

  /** Protocol version */
  protocolVersion: number;

  /** Client capability flags */
  capabilityFlags: number;
}

/**
 * Capability flags for compact storage
 */
export const CapabilityFlags = {
  BINARY_PROTOCOL: 0x01,
  COMPRESSION: 0x02,
  BATCHING: 0x04,
} as const;

/**
 * Encode capabilities to flags
 */
export function encodeCapabilities(caps: ClientCapabilities): number {
  let flags = 0;
  if (caps.binaryProtocol) flags |= CapabilityFlags.BINARY_PROTOCOL;
  if (caps.compression) flags |= CapabilityFlags.COMPRESSION;
  if (caps.batching) flags |= CapabilityFlags.BATCHING;
  return flags;
}

/**
 * Decode flags to capabilities
 */
export function decodeCapabilities(flags: number): Partial<ClientCapabilities> {
  return {
    binaryProtocol: (flags & CapabilityFlags.BINARY_PROTOCOL) !== 0,
    compression: (flags & CapabilityFlags.COMPRESSION) !== 0,
    batching: (flags & CapabilityFlags.BATCHING) !== 0,
  };
}

// =============================================================================
// Iceberg Types
// =============================================================================

/**
 * Iceberg table schema field
 */
export interface IcebergField {
  id: number;
  name: string;
  type: string;
  required: boolean;
  doc?: string;
}

/**
 * Iceberg table schema
 */
export interface IcebergSchema {
  type: 'struct';
  'schema-id': number;
  fields: IcebergField[];
  'identifier-field-ids'?: number[];
}

/**
 * Iceberg partition field
 */
export interface IcebergPartitionField {
  'source-id': number;
  'field-id': number;
  name: string;
  transform: string;
}

/**
 * Iceberg partition spec
 */
export interface IcebergPartitionSpec {
  'spec-id': number;
  fields: IcebergPartitionField[];
}

/**
 * Iceberg sort field
 */
export interface IcebergSortField {
  transform: string;
  'source-id': number;
  direction: 'asc' | 'desc';
  'null-order': 'nulls-first' | 'nulls-last';
}

/**
 * Iceberg sort order
 */
export interface IcebergSortOrder {
  'order-id': number;
  fields: IcebergSortField[];
}

/**
 * Snapshot summary
 */
export interface SnapshotSummary {
  operation: 'append' | 'replace' | 'overwrite' | 'delete';
  'added-data-files'?: string;
  'added-records'?: string;
  'added-files-size'?: string;
  'deleted-data-files'?: string;
  'deleted-records'?: string;
  'total-records'?: string;
  'total-files-size'?: string;
  'total-data-files'?: string;
  [key: string]: string | undefined;
}

/**
 * Iceberg snapshot
 */
export interface IcebergSnapshot {
  'snapshot-id': bigint;
  'parent-snapshot-id': bigint | null;
  'sequence-number': bigint;
  'timestamp-ms': bigint;
  'manifest-list': string;
  summary: SnapshotSummary;
  'schema-id'?: number;
}

/**
 * Manifest file entry
 */
export interface ManifestFile {
  'manifest-path': string;
  'manifest-length': bigint;
  'partition-spec-id': number;
  content: 'data' | 'deletes';
  'sequence-number': bigint;
  'min-sequence-number': bigint;
  'added-snapshot-id': bigint;
  'added-files-count': number;
  'existing-files-count': number;
  'deleted-files-count': number;
  'added-rows-count': bigint;
  'existing-rows-count': bigint;
  'deleted-rows-count': bigint;
}

/**
 * Data file entry
 */
export interface DataFile {
  content: 0 | 1 | 2;
  'file-path': string;
  'file-format': 'parquet' | 'avro' | 'orc';
  partition: Record<string, unknown>;
  'record-count': bigint;
  'file-size-in-bytes': bigint;
  'column-sizes'?: Map<number, bigint>;
  'value-counts'?: Map<number, bigint>;
  'null-value-counts'?: Map<number, bigint>;
  'lower-bounds'?: Map<number, Uint8Array>;
  'upper-bounds'?: Map<number, Uint8Array>;
  'sort-order-id'?: number;
}

/**
 * Iceberg table metadata
 */
export interface IcebergTableMetadata {
  'format-version': 2;
  'table-uuid': string;
  location: string;
  'last-sequence-number': bigint;
  'last-updated-ms': bigint;
  'last-column-id': number;
  'last-partition-id'?: number;
  'current-schema-id': number;
  schemas: IcebergSchema[];
  'default-spec-id': number;
  'partition-specs': IcebergPartitionSpec[];
  'default-sort-order-id': number;
  'sort-orders': IcebergSortOrder[];
  properties?: Record<string, string>;
  'current-snapshot-id': bigint | null;
  snapshots: IcebergSnapshot[];
  'snapshot-log': Array<{
    'snapshot-id': bigint;
    'timestamp-ms': bigint;
  }>;
  'metadata-log'?: Array<{
    'metadata-file': string;
    'timestamp-ms': bigint;
  }>;
  refs?: Record<string, {
    'snapshot-id': bigint;
    type: 'branch' | 'tag';
  }>;
}

// =============================================================================
// Namespace Types for REST Catalog
// =============================================================================

/**
 * Namespace identifier
 */
export type NamespaceIdentifier = string[];

/**
 * Namespace properties
 */
export interface NamespaceProperties {
  [key: string]: string;
}

/**
 * Table identifier
 */
export interface TableIdentifier {
  namespace: NamespaceIdentifier;
  name: string;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Base error class for DoLake
 */
export class DoLakeError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly retryable: boolean = false
  ) {
    super(message);
    this.name = 'DoLakeError';
  }
}

/**
 * Connection error
 */
export class ConnectionError extends DoLakeError {
  constructor(message: string, retryable: boolean = true) {
    super(message, 'CONNECTION_ERROR', retryable);
    this.name = 'ConnectionError';
  }
}

/**
 * Buffer overflow error
 */
export class BufferOverflowError extends DoLakeError {
  constructor(message: string) {
    super(message, 'BUFFER_OVERFLOW', true);
    this.name = 'BufferOverflowError';
  }
}

/**
 * Flush error
 */
export class FlushError extends DoLakeError {
  constructor(message: string, public readonly usedFallback: boolean) {
    super(message, 'FLUSH_ERROR', true);
    this.name = 'FlushError';
  }
}

/**
 * Parquet write error
 */
export class ParquetWriteError extends DoLakeError {
  constructor(message: string) {
    super(message, 'PARQUET_WRITE_ERROR', true);
    this.name = 'ParquetWriteError';
  }
}

/**
 * Iceberg metadata error
 */
export class IcebergError extends DoLakeError {
  constructor(message: string) {
    super(message, 'ICEBERG_ERROR', false);
    this.name = 'IcebergError';
  }
}

// =============================================================================
// Utility Types
// =============================================================================

/**
 * Union of client messages
 */
export type ClientRpcMessage =
  | CDCBatchMessage
  | ConnectMessage
  | HeartbeatMessage
  | FlushRequestMessage;

/**
 * Union of server messages
 */
export type ServerRpcMessage = AckMessage | NackMessage | StatusMessage;

/**
 * All RPC message types
 */
export type AnyRpcMessage = ClientRpcMessage | ServerRpcMessage;

/**
 * Type guards
 */
export function isCDCBatchMessage(msg: RpcMessage): msg is CDCBatchMessage {
  return msg.type === 'cdc_batch';
}

export function isAckMessage(msg: RpcMessage): msg is AckMessage {
  return msg.type === 'ack';
}

export function isNackMessage(msg: RpcMessage): msg is NackMessage {
  return msg.type === 'nack';
}

export function isConnectMessage(msg: RpcMessage): msg is ConnectMessage {
  return msg.type === 'connect';
}

export function isHeartbeatMessage(msg: RpcMessage): msg is HeartbeatMessage {
  return msg.type === 'heartbeat';
}

export function isFlushRequestMessage(msg: RpcMessage): msg is FlushRequestMessage {
  return msg.type === 'flush_request';
}

/**
 * Generate a unique batch ID
 */
export function generateBatchId(sourceDoId: string, sequence: number): string {
  return `${sourceDoId.slice(0, 8)}_${sequence}_${Date.now().toString(36)}`;
}

/**
 * Generate a unique correlation ID
 */
export function generateCorrelationId(): string {
  return `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

/**
 * Generate a unique snapshot ID
 */
export function generateSnapshotId(): bigint {
  const timestamp = BigInt(Date.now());
  const random = BigInt(Math.floor(Math.random() * 1000000));
  return timestamp * 1000000n + random;
}

/**
 * Generate a UUID v4
 */
export function generateUUID(): string {
  return crypto.randomUUID();
}
