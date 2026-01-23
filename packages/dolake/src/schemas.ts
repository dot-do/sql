/**
 * DoLake Zod Schemas
 *
 * Zod validation schemas for WebSocket RPC messages.
 * Provides runtime type validation for all incoming messages.
 */

import { z, ZodError } from 'zod';

// =============================================================================
// CDC Event Schemas
// =============================================================================

/**
 * CDC operation types
 */
export const CDCOperationSchema = z.enum(['INSERT', 'UPDATE', 'DELETE']);

/**
 * CDC Event schema
 */
export const CDCEventSchema = z.object({
  /** Monotonically increasing sequence number */
  sequence: z.number().int(),
  /** Unix timestamp in milliseconds */
  timestamp: z.number().int(),
  /** Type of database operation */
  operation: CDCOperationSchema,
  /** Table name where the change occurred */
  table: z.string().min(1),
  /** Primary key or row identifier */
  rowId: z.string(),
  /** Row data before the change (for UPDATE/DELETE) */
  before: z.unknown().optional(),
  /** Row data after the change (for INSERT/UPDATE) */
  after: z.unknown().optional(),
  /** Optional metadata */
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export type ValidatedCDCEvent = z.infer<typeof CDCEventSchema>;

// =============================================================================
// Client Capabilities Schema
// =============================================================================

/**
 * Client capabilities schema
 */
export const ClientCapabilitiesSchema = z.object({
  binaryProtocol: z.boolean(),
  compression: z.boolean(),
  batching: z.boolean(),
  maxBatchSize: z.number().int().positive(),
  maxMessageSize: z.number().int().positive(),
});

export type ValidatedClientCapabilities = z.infer<typeof ClientCapabilitiesSchema>;

// =============================================================================
// RPC Message Schemas
// =============================================================================

/**
 * Base fields shared by all messages
 */
const BaseMessageFields = {
  timestamp: z.number().int(),
  correlationId: z.string().optional(),
};

/**
 * CDC Batch message schema
 */
export const CDCBatchMessageSchema = z.object({
  type: z.literal('cdc_batch'),
  ...BaseMessageFields,
  /** ID of the source DoSQL Durable Object */
  sourceDoId: z.string().min(1),
  /** Human-readable name of the source shard */
  sourceShardName: z.string().optional(),
  /** CDC events in this batch */
  events: z.array(CDCEventSchema),
  /** Sequence number of this batch */
  sequenceNumber: z.number().int(),
  /** First event sequence in this batch */
  firstEventSequence: z.number().int(),
  /** Last event sequence in this batch */
  lastEventSequence: z.number().int(),
  /** Total size of events in bytes (approximate) */
  sizeBytes: z.number().int(),
  /** Whether this is a retry */
  isRetry: z.boolean(),
  /** Retry count */
  retryCount: z.number().int(),
});

export type ValidatedCDCBatchMessage = z.infer<typeof CDCBatchMessageSchema>;

/**
 * Connect message schema
 */
export const ConnectMessageSchema = z.object({
  type: z.literal('connect'),
  ...BaseMessageFields,
  /** ID of the connecting DoSQL DO */
  sourceDoId: z.string().min(1),
  /** Shard name */
  sourceShardName: z.string().optional(),
  /** Last acknowledged sequence number (for resumption) */
  lastAckSequence: z.number().int(),
  /** Protocol version */
  protocolVersion: z.number().int().nonnegative(),
  /** Client capabilities */
  capabilities: ClientCapabilitiesSchema,
});

export type ValidatedConnectMessage = z.infer<typeof ConnectMessageSchema>;

/**
 * Heartbeat message schema
 */
export const HeartbeatMessageSchema = z.object({
  type: z.literal('heartbeat'),
  ...BaseMessageFields,
  /** ID of the source DO */
  sourceDoId: z.string().min(1),
  /** Last acknowledged sequence */
  lastAckSequence: z.number().int(),
  /** Pending events count */
  pendingEvents: z.number().int(),
});

export type ValidatedHeartbeatMessage = z.infer<typeof HeartbeatMessageSchema>;

/**
 * Flush request reasons
 */
export const FlushReasonSchema = z.enum([
  'manual',
  'shutdown',
  'buffer_full',
  'time_threshold',
]);

/**
 * Flush request message schema
 */
export const FlushRequestMessageSchema = z.object({
  type: z.literal('flush_request'),
  ...BaseMessageFields,
  /** ID of the requesting DO */
  sourceDoId: z.string().min(1),
  /** Reason for flush */
  reason: FlushReasonSchema,
});

export type ValidatedFlushRequestMessage = z.infer<typeof FlushRequestMessageSchema>;

/**
 * Disconnect message schema
 */
export const DisconnectMessageSchema = z.object({
  type: z.literal('disconnect'),
  ...BaseMessageFields,
  /** ID of the disconnecting DO */
  sourceDoId: z.string().min(1).optional(),
  /** Reason for disconnect */
  reason: z.string().optional(),
});

export type ValidatedDisconnectMessage = z.infer<typeof DisconnectMessageSchema>;

// =============================================================================
// Server Response Schemas (for validation of outgoing messages, if needed)
// =============================================================================

/**
 * Ack status
 */
export const AckStatusSchema = z.enum([
  'ok',
  'buffered',
  'persisted',
  'duplicate',
  'fallback',
]);

/**
 * Ack details schema
 */
export const AckDetailsSchema = z.object({
  eventsProcessed: z.number().int(),
  bufferUtilization: z.number(),
  timeUntilFlush: z.number().optional(),
  persistedPath: z.string().optional(),
});

/**
 * Ack message schema
 */
export const AckMessageSchema = z.object({
  type: z.literal('ack'),
  ...BaseMessageFields,
  sequenceNumber: z.number().int(),
  status: AckStatusSchema,
  batchId: z.string().optional(),
  details: AckDetailsSchema.optional(),
});

export type ValidatedAckMessage = z.infer<typeof AckMessageSchema>;

/**
 * Nack reasons
 */
export const NackReasonSchema = z.enum([
  'buffer_full',
  'rate_limited',
  'invalid_sequence',
  'invalid_format',
  'internal_error',
  'shutting_down',
]);

/**
 * Nack message schema
 */
export const NackMessageSchema = z.object({
  type: z.literal('nack'),
  ...BaseMessageFields,
  sequenceNumber: z.number().int(),
  reason: NackReasonSchema,
  errorMessage: z.string(),
  shouldRetry: z.boolean(),
  retryDelayMs: z.number().int().optional(),
});

export type ValidatedNackMessage = z.infer<typeof NackMessageSchema>;

/**
 * DoLake state
 */
export const DoLakeStateSchema = z.enum([
  'idle',
  'receiving',
  'flushing',
  'recovering',
  'error',
]);

/**
 * Buffer stats schema
 */
export const BufferStatsSchema = z.object({
  batchCount: z.number().int(),
  eventCount: z.number().int(),
  totalSizeBytes: z.number().int(),
  utilization: z.number(),
  oldestBatchTime: z.number().optional(),
  newestBatchTime: z.number().optional(),
});

/**
 * Status message schema
 */
export const StatusMessageSchema = z.object({
  type: z.literal('status'),
  ...BaseMessageFields,
  state: DoLakeStateSchema,
  buffer: BufferStatsSchema,
  connectedSources: z.number().int(),
  lastFlushTime: z.number().optional(),
  nextFlushTime: z.number().optional(),
});

export type ValidatedStatusMessage = z.infer<typeof StatusMessageSchema>;

// =============================================================================
// Discriminated Union for All Client RPC Messages
// =============================================================================

/**
 * Union of all valid client RPC messages
 */
export const ClientRpcMessageSchema = z.discriminatedUnion('type', [
  CDCBatchMessageSchema,
  ConnectMessageSchema,
  HeartbeatMessageSchema,
  FlushRequestMessageSchema,
  DisconnectMessageSchema,
]);

export type ValidatedClientRpcMessage = z.infer<typeof ClientRpcMessageSchema>;

/**
 * Union of all valid RPC messages (client + server)
 */
export const RpcMessageSchema = z.discriminatedUnion('type', [
  // Client messages
  CDCBatchMessageSchema,
  ConnectMessageSchema,
  HeartbeatMessageSchema,
  FlushRequestMessageSchema,
  DisconnectMessageSchema,
  // Server messages
  AckMessageSchema,
  NackMessageSchema,
  StatusMessageSchema,
]);

export type ValidatedRpcMessage = z.infer<typeof RpcMessageSchema>;

// =============================================================================
// Validation Functions
// =============================================================================

/**
 * Error class for WebSocket RPC message validation failures.
 *
 * This error is thrown when an incoming WebSocket message fails Zod schema validation.
 * It wraps the underlying ZodError and provides methods for extracting detailed
 * validation failure information.
 *
 * ## When It's Thrown
 *
 * - When `validateClientMessage()` receives an invalid message
 * - When `validateRpcMessage()` receives an invalid message
 * - When type-specific validators (e.g., `validateCDCBatchMessage()`) fail
 * - When JSON parsing fails during message decoding in WebSocketHandler
 *
 * ## Error Format
 *
 * The error provides:
 * - `message`: A summary of the validation failure
 * - `zodError`: The underlying Zod validation error (null for JSON parse errors)
 * - `name`: Always `'MessageValidationError'`
 *
 * When sent as a WebSocket NACK response, the error format is:
 * ```json
 * {
 *   "type": "nack",
 *   "timestamp": 1705329600000,
 *   "sequenceNumber": 0,
 *   "reason": "invalid_format",
 *   "errorMessage": "sourceDoId: Required; events: Expected array, received object",
 *   "shouldRetry": false
 * }
 * ```
 *
 * ## Handling the Error
 *
 * ```typescript
 * import { validateClientMessage, MessageValidationError } from 'dolake';
 *
 * try {
 *   const message = validateClientMessage(rawData);
 *   // Process valid message
 * } catch (error) {
 *   if (error instanceof MessageValidationError) {
 *     // Get detailed field-level errors
 *     console.error('Validation failed:', error.getErrorDetails());
 *     // Output: "sourceDoId: Required; events: Expected array, received object"
 *
 *     // Access individual Zod issues for programmatic handling
 *     if (error.zodError) {
 *       for (const issue of error.zodError.issues) {
 *         console.log(`Field: ${issue.path.join('.')}, Error: ${issue.message}`);
 *       }
 *     }
 *   }
 * }
 * ```
 *
 * ## Common Validation Failures
 *
 * | Failure | Example Error |
 * |---------|---------------|
 * | Missing required field | `sourceDoId: Required` |
 * | Wrong type | `timestamp: Expected number, received string` |
 * | Invalid enum value | `operation: Invalid enum value. Expected 'INSERT' \| 'UPDATE' \| 'DELETE'` |
 * | Invalid array | `events: Expected array, received object` |
 * | Malformed JSON | `Failed to parse JSON: Unexpected token` |
 * | Unknown message type | `Invalid discriminator value. Expected 'cdc_batch' \| 'connect' \| ...` |
 *
 * @example
 * // Catching and handling validation errors
 * try {
 *   const msg = validateCDCBatchMessage(data);
 * } catch (error) {
 *   if (error instanceof MessageValidationError) {
 *     // Log detailed error for debugging
 *     console.error(error.getErrorDetails());
 *
 *     // Check if it's a JSON parse error vs schema validation error
 *     if (error.zodError === null) {
 *       console.error('JSON parsing failed');
 *     } else {
 *       console.error('Schema validation failed');
 *     }
 *   }
 * }
 *
 * @see validateClientMessage
 * @see validateRpcMessage
 * @see CDCBatchMessageSchema
 */
export class MessageValidationError extends Error {
  /**
   * The underlying Zod validation error, if available.
   *
   * This is `null` when the error is due to JSON parsing failure rather than
   * schema validation failure. When present, it contains detailed information
   * about which fields failed validation and why.
   *
   * @example
   * if (error.zodError) {
   *   for (const issue of error.zodError.issues) {
   *     console.log(`Path: ${issue.path.join('.')}`);
   *     console.log(`Code: ${issue.code}`);
   *     console.log(`Message: ${issue.message}`);
   *   }
   * }
   */
  public readonly zodError: ZodError | null;

  /**
   * Creates a new MessageValidationError.
   *
   * @param message - A human-readable description of the validation failure
   * @param zodError - The underlying Zod error, if this is a schema validation failure.
   *                   Pass `null` for JSON parse errors or other non-schema errors.
   *
   * @example
   * // For schema validation failures
   * const result = CDCBatchMessageSchema.safeParse(data);
   * if (!result.success) {
   *   throw new MessageValidationError(
   *     `Invalid CDC batch: ${result.error.issues.map(i => i.message).join(', ')}`,
   *     result.error
   *   );
   * }
   *
   * @example
   * // For JSON parse failures
   * try {
   *   JSON.parse(rawMessage);
   * } catch (parseError) {
   *   throw new MessageValidationError(
   *     `Failed to parse JSON: ${parseError.message}`,
   *     null  // No Zod error for parse failures
   *   );
   * }
   */
  constructor(message: string, zodError?: ZodError | null) {
    super(message);
    this.name = 'MessageValidationError';
    this.zodError = zodError ?? null;
  }

  /**
   * Get a human-readable description of all validation errors.
   *
   * Returns a semicolon-separated string of field paths and their error messages.
   * For JSON parse errors (when `zodError` is null), returns the original error message.
   *
   * @returns A formatted string describing all validation errors
   *
   * @example
   * // For a message missing multiple required fields:
   * error.getErrorDetails();
   * // Returns: "sourceDoId: Required; events: Required; sequenceNumber: Required"
   *
   * @example
   * // For type mismatches:
   * error.getErrorDetails();
   * // Returns: "timestamp: Expected number, received string; isRetry: Expected boolean"
   *
   * @example
   * // For JSON parse errors:
   * error.getErrorDetails();
   * // Returns: "Failed to parse JSON: Unexpected token 'x' at position 5"
   */
  getErrorDetails(): string {
    if (!this.zodError) {
      return this.message;
    }
    return this.zodError.issues
      .map((e) => `${e.path.join('.')}: ${e.message}`)
      .join('; ');
  }
}

/**
 * Validate and parse a client RPC message
 *
 * @param data - Raw parsed JSON data
 * @returns Validated and typed message
 * @throws MessageValidationError if validation fails
 */
export function validateClientMessage(data: unknown): ValidatedClientRpcMessage {
  const result = ClientRpcMessageSchema.safeParse(data);

  if (!result.success) {
    throw new MessageValidationError(
      `Invalid message format: ${result.error.issues.map((e) => e.message).join(', ')}`,
      result.error
    );
  }

  return result.data;
}

/**
 * Validate and parse any RPC message
 *
 * @param data - Raw parsed JSON data
 * @returns Validated and typed message
 * @throws MessageValidationError if validation fails
 */
export function validateRpcMessage(data: unknown): ValidatedRpcMessage {
  const result = RpcMessageSchema.safeParse(data);

  if (!result.success) {
    throw new MessageValidationError(
      `Invalid message format: ${result.error.issues.map((e) => e.message).join(', ')}`,
      result.error
    );
  }

  return result.data;
}

/**
 * Check if data is a valid client RPC message (non-throwing)
 *
 * @param data - Raw data to check
 * @returns True if valid, false otherwise
 */
export function isValidClientMessage(data: unknown): data is ValidatedClientRpcMessage {
  return ClientRpcMessageSchema.safeParse(data).success;
}

/**
 * Check if data is a valid RPC message (non-throwing)
 *
 * @param data - Raw data to check
 * @returns True if valid, false otherwise
 */
export function isValidRpcMessage(data: unknown): data is ValidatedRpcMessage {
  return RpcMessageSchema.safeParse(data).success;
}

/**
 * Validate specific message types
 */
export const validateCDCBatchMessage = (data: unknown): ValidatedCDCBatchMessage => {
  const result = CDCBatchMessageSchema.safeParse(data);
  if (!result.success) {
    throw new MessageValidationError(
      `Invalid CDC batch message: ${result.error.issues.map((e) => e.message).join(', ')}`,
      result.error
    );
  }
  return result.data;
};

export const validateConnectMessage = (data: unknown): ValidatedConnectMessage => {
  const result = ConnectMessageSchema.safeParse(data);
  if (!result.success) {
    throw new MessageValidationError(
      `Invalid connect message: ${result.error.issues.map((e) => e.message).join(', ')}`,
      result.error
    );
  }
  return result.data;
};

export const validateHeartbeatMessage = (data: unknown): ValidatedHeartbeatMessage => {
  const result = HeartbeatMessageSchema.safeParse(data);
  if (!result.success) {
    throw new MessageValidationError(
      `Invalid heartbeat message: ${result.error.issues.map((e) => e.message).join(', ')}`,
      result.error
    );
  }
  return result.data;
};

export const validateFlushRequestMessage = (data: unknown): ValidatedFlushRequestMessage => {
  const result = FlushRequestMessageSchema.safeParse(data);
  if (!result.success) {
    throw new MessageValidationError(
      `Invalid flush request message: ${result.error.issues.map((e) => e.message).join(', ')}`,
      result.error
    );
  }
  return result.data;
};
