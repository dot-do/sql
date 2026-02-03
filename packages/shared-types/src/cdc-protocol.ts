/**
 * CDC Protocol - Formal contract between DoSQL and DoLake
 *
 * This module defines the canonical CDC (Change Data Capture) protocol for streaming
 * changes from DoSQL to DoLake. It provides:
 *
 * - Versioned protocol definitions for backward/forward compatibility
 * - Formal CDCEvent interface with version field
 * - Validation functions for CDC events
 * - Protocol version negotiation types
 *
 * ## Protocol Versioning
 *
 * The CDC protocol uses semantic versioning:
 * - MAJOR version changes indicate breaking changes
 * - MINOR version changes add new optional fields
 * - Version negotiation ensures compatible communication
 *
 * ## CDC Event Contract
 *
 * Every CDC event MUST include:
 * - `version`: Protocol version number
 * - `type`: Operation type ('INSERT' | 'UPDATE' | 'DELETE')
 * - `table`: Table name affected
 * - `lsn`: Log Sequence Number (bigint)
 * - `timestamp`: Unix timestamp in milliseconds
 *
 * Events MAY include:
 * - `before`: Row data before change (UPDATE, DELETE)
 * - `after`: Row data after change (INSERT, UPDATE)
 * - `transactionId`: Associated transaction
 * - `schemaVersion`: Schema version for the table
 * - `primaryKey`: Primary key values
 *
 * @packageDocumentation
 * @module cdc-protocol
 * @stability experimental
 * @since 0.4.0
 */

import type { LSN, TransactionId, SchemaVersion } from './index.js';

// =============================================================================
// Protocol Version Constants
// =============================================================================

/**
 * Current CDC protocol version.
 *
 * Increment this when making changes to the protocol:
 * - Bump MAJOR (e.g., 1 -> 2) for breaking changes
 * - Bump MINOR (e.g., 1 -> 1.1) for new optional fields
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const CDC_PROTOCOL_VERSION = 1;

/**
 * Minimum supported CDC protocol version.
 *
 * Consumers will reject events with versions below this threshold.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const CDC_PROTOCOL_MIN_VERSION = 1;

/**
 * Protocol version history for documentation and compatibility checks.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const CDC_PROTOCOL_HISTORY = [
  {
    version: 1,
    releaseDate: '2026-02-03',
    description: 'Initial versioned protocol with formal CDC event contract',
    changes: [
      'Added version field to CDCEvent',
      'Formalized event types as INSERT | UPDATE | DELETE',
      'Added protocol negotiation support',
      'Added validation functions',
    ],
  },
] as const;

// =============================================================================
// CDC Event Types
// =============================================================================

/**
 * CDC operation types for the protocol.
 *
 * - `INSERT`: New row was added
 * - `UPDATE`: Existing row was modified
 * - `DELETE`: Row was removed
 *
 * Note: TRUNCATE is intentionally excluded from the protocol as it is a DDL
 * operation that should be handled separately via schema change events.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type CDCEventType = 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Row data type alias for CDC events.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type Row = Record<string, unknown>;

/**
 * Versioned CDC Event - The canonical event format for the CDC protocol.
 *
 * This is the formal contract between DoSQL (producer) and DoLake (consumer).
 * All CDC events streamed between these systems MUST conform to this interface.
 *
 * ## Required Fields
 *
 * - `version`: Protocol version number (MUST match CDC_PROTOCOL_VERSION)
 * - `type`: Operation type ('INSERT', 'UPDATE', 'DELETE')
 * - `table`: Name of the affected table
 * - `lsn`: Log Sequence Number for ordering and deduplication
 * - `timestamp`: Unix timestamp in milliseconds when change occurred
 *
 * ## Optional Fields
 *
 * - `before`: Row data before the change (required for UPDATE, DELETE)
 * - `after`: Row data after the change (required for INSERT, UPDATE)
 * - `transactionId`: Transaction ID if change is part of a transaction
 * - `schemaVersion`: Schema version for the table at time of change
 * - `primaryKey`: Primary key values for efficient lookups
 *
 * @typeParam T - Row data type (defaults to Record<string, unknown>)
 *
 * @example INSERT event
 * ```typescript
 * const insertEvent: VersionedCDCEvent<User> = {
 *   version: 1,
 *   type: 'INSERT',
 *   table: 'users',
 *   lsn: 1001n,
 *   timestamp: Date.now(),
 *   after: { id: 1, name: 'Alice', email: 'alice@example.com' },
 *   primaryKey: { id: 1 },
 * };
 * ```
 *
 * @example UPDATE event
 * ```typescript
 * const updateEvent: VersionedCDCEvent<User> = {
 *   version: 1,
 *   type: 'UPDATE',
 *   table: 'users',
 *   lsn: 1002n,
 *   timestamp: Date.now(),
 *   before: { id: 1, name: 'Alice', email: 'alice@example.com' },
 *   after: { id: 1, name: 'Alice Smith', email: 'alice@example.com' },
 *   primaryKey: { id: 1 },
 *   transactionId: 'tx-123',
 * };
 * ```
 *
 * @example DELETE event
 * ```typescript
 * const deleteEvent: VersionedCDCEvent<User> = {
 *   version: 1,
 *   type: 'DELETE',
 *   table: 'users',
 *   lsn: 1003n,
 *   timestamp: Date.now(),
 *   before: { id: 1, name: 'Alice Smith', email: 'alice@example.com' },
 *   primaryKey: { id: 1 },
 * };
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export interface VersionedCDCEvent<T = Row> {
  // === Protocol Version ===
  /**
   * CDC protocol version number.
   *
   * This MUST match CDC_PROTOCOL_VERSION for the event to be valid.
   * Consumers SHOULD validate this before processing events.
   */
  version: number;

  // === Event Type ===
  /**
   * Type of change operation.
   *
   * - INSERT: New row added to table
   * - UPDATE: Existing row modified
   * - DELETE: Row removed from table
   */
  type: CDCEventType;

  // === Identification ===
  /**
   * Name of the affected table.
   */
  table: string;

  /**
   * Log Sequence Number for ordering and deduplication.
   *
   * LSNs are monotonically increasing within a database instance.
   * Consumers can use LSN for:
   * - Ordering events correctly
   * - Deduplication on retry
   * - Resuming from a specific position
   */
  lsn: bigint | LSN;

  /**
   * Unix timestamp in milliseconds when the change occurred.
   *
   * This is the time the change was committed, not when it was sent.
   */
  timestamp: number;

  // === Row Data ===
  /**
   * Row data before the change.
   *
   * - Required for UPDATE events (shows old values)
   * - Required for DELETE events (shows deleted row)
   * - Not present for INSERT events
   */
  before?: T;

  /**
   * Row data after the change.
   *
   * - Required for INSERT events (shows new row)
   * - Required for UPDATE events (shows new values)
   * - Not present for DELETE events
   */
  after?: T;

  // === Transaction Context ===
  /**
   * Transaction ID if the change is part of a transaction.
   *
   * Changes within the same transaction will share this ID.
   */
  transactionId?: string | TransactionId;

  // === Schema Information ===
  /**
   * Schema version of the table at the time of change.
   *
   * Useful for schema evolution tracking and compatibility checks.
   */
  schemaVersion?: number | SchemaVersion;

  /**
   * Primary key values for the affected row.
   *
   * Enables efficient lookups and conflict resolution.
   */
  primaryKey?: Record<string, unknown>;

  // === Extension Point ===
  /**
   * Optional metadata for custom extensions.
   *
   * Producers MAY add custom metadata here.
   * Consumers MUST NOT fail on unknown metadata keys.
   */
  metadata?: Record<string, unknown>;
}

// =============================================================================
// Protocol Negotiation Types
// =============================================================================

/**
 * Protocol capabilities advertised during negotiation.
 *
 * @public
 * @stability experimental
 * @since 0.4.0
 */
export interface CDCProtocolCapabilities {
  /** Supported protocol version */
  version: number;
  /** Minimum supported version (for backward compatibility) */
  minVersion: number;
  /** Maximum supported version (for forward compatibility) */
  maxVersion: number;
  /** Whether compression is supported */
  supportsCompression: boolean;
  /** Whether binary encoding is supported */
  supportsBinary: boolean;
  /** Whether batching is supported */
  supportsBatching: boolean;
  /** Maximum batch size if batching is supported */
  maxBatchSize?: number;
}

/**
 * Protocol negotiation request from producer to consumer.
 *
 * @public
 * @stability experimental
 * @since 0.4.0
 */
export interface CDCProtocolNegotiationRequest {
  /** Type discriminator */
  type: 'protocol_negotiation';
  /** Producer's capabilities */
  capabilities: CDCProtocolCapabilities;
  /** Producer's identifier */
  producerId: string;
  /** Timestamp of the request */
  timestamp: number;
}

/**
 * Protocol negotiation response from consumer to producer.
 *
 * @public
 * @stability experimental
 * @since 0.4.0
 */
export interface CDCProtocolNegotiationResponse {
  /** Type discriminator */
  type: 'protocol_negotiation_response';
  /** Whether negotiation was successful */
  success: boolean;
  /** Negotiated protocol version (highest common version) */
  negotiatedVersion: number;
  /** Consumer's capabilities */
  capabilities: CDCProtocolCapabilities;
  /** Error message if negotiation failed */
  error?: string;
  /** Timestamp of the response */
  timestamp: number;
}

/**
 * Default protocol capabilities for producers.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const DEFAULT_PRODUCER_CAPABILITIES: Readonly<CDCProtocolCapabilities> = {
  version: CDC_PROTOCOL_VERSION,
  minVersion: CDC_PROTOCOL_MIN_VERSION,
  maxVersion: CDC_PROTOCOL_VERSION,
  supportsCompression: true,
  supportsBinary: true,
  supportsBatching: true,
  maxBatchSize: 1000,
};

/**
 * Default protocol capabilities for consumers.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const DEFAULT_CONSUMER_CAPABILITIES: Readonly<CDCProtocolCapabilities> = {
  version: CDC_PROTOCOL_VERSION,
  minVersion: CDC_PROTOCOL_MIN_VERSION,
  maxVersion: CDC_PROTOCOL_VERSION,
  supportsCompression: true,
  supportsBinary: true,
  supportsBatching: true,
  maxBatchSize: 1000,
};

// =============================================================================
// Validation Types
// =============================================================================

/**
 * Result of CDC event validation.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export interface CDCValidationResult {
  /** Whether the event is valid */
  valid: boolean;
  /** Validation errors if not valid */
  errors: CDCValidationError[];
  /** Validation warnings (non-fatal issues) */
  warnings: CDCValidationWarning[];
}

/**
 * Validation error detail.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export interface CDCValidationError {
  /** Error code for programmatic handling */
  code: CDCValidationErrorCode;
  /** Human-readable error message */
  message: string;
  /** Field that caused the error */
  field?: string;
  /** Expected value or format */
  expected?: string;
  /** Actual value received */
  actual?: string;
}

/**
 * Validation warning detail.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export interface CDCValidationWarning {
  /** Warning code for programmatic handling */
  code: CDCValidationWarningCode;
  /** Human-readable warning message */
  message: string;
  /** Field that caused the warning */
  field?: string;
}

/**
 * CDC validation error codes.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export enum CDCValidationErrorCode {
  /** Protocol version missing */
  MISSING_VERSION = 'MISSING_VERSION',
  /** Protocol version not supported */
  UNSUPPORTED_VERSION = 'UNSUPPORTED_VERSION',
  /** Event type missing */
  MISSING_TYPE = 'MISSING_TYPE',
  /** Event type invalid */
  INVALID_TYPE = 'INVALID_TYPE',
  /** Table name missing */
  MISSING_TABLE = 'MISSING_TABLE',
  /** Table name invalid */
  INVALID_TABLE = 'INVALID_TABLE',
  /** LSN missing */
  MISSING_LSN = 'MISSING_LSN',
  /** LSN invalid format */
  INVALID_LSN = 'INVALID_LSN',
  /** Timestamp missing */
  MISSING_TIMESTAMP = 'MISSING_TIMESTAMP',
  /** Timestamp invalid */
  INVALID_TIMESTAMP = 'INVALID_TIMESTAMP',
  /** INSERT event missing 'after' data */
  INSERT_MISSING_AFTER = 'INSERT_MISSING_AFTER',
  /** UPDATE event missing 'before' data */
  UPDATE_MISSING_BEFORE = 'UPDATE_MISSING_BEFORE',
  /** UPDATE event missing 'after' data */
  UPDATE_MISSING_AFTER = 'UPDATE_MISSING_AFTER',
  /** DELETE event missing 'before' data */
  DELETE_MISSING_BEFORE = 'DELETE_MISSING_BEFORE',
}

/**
 * CDC validation warning codes.
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export enum CDCValidationWarningCode {
  /** Schema version not specified */
  MISSING_SCHEMA_VERSION = 'MISSING_SCHEMA_VERSION',
  /** Primary key not specified */
  MISSING_PRIMARY_KEY = 'MISSING_PRIMARY_KEY',
  /** Transaction ID not specified */
  MISSING_TRANSACTION_ID = 'MISSING_TRANSACTION_ID',
  /** Event has unknown metadata keys */
  UNKNOWN_METADATA_KEYS = 'UNKNOWN_METADATA_KEYS',
  /** Timestamp is in the future */
  FUTURE_TIMESTAMP = 'FUTURE_TIMESTAMP',
  /** Timestamp is too old */
  OLD_TIMESTAMP = 'OLD_TIMESTAMP',
}

// =============================================================================
// Validation Functions
// =============================================================================

/**
 * Valid CDC event types.
 */
const VALID_EVENT_TYPES = new Set<CDCEventType>(['INSERT', 'UPDATE', 'DELETE']);

/**
 * Validates a CDC event against the protocol specification.
 *
 * Checks for:
 * - Required fields (version, type, table, lsn, timestamp)
 * - Correct version number
 * - Valid event type
 * - Appropriate before/after data for event type
 *
 * @param event - The CDC event to validate
 * @param options - Validation options
 * @returns Validation result with errors and warnings
 *
 * @example
 * ```typescript
 * const event: VersionedCDCEvent = {
 *   version: 1,
 *   type: 'INSERT',
 *   table: 'users',
 *   lsn: 1001n,
 *   timestamp: Date.now(),
 *   after: { id: 1, name: 'Alice' },
 * };
 *
 * const result = validateCDCEvent(event);
 * if (!result.valid) {
 *   console.error('Validation errors:', result.errors);
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function validateCDCEvent(
  event: unknown,
  options: {
    /** Minimum supported version (default: CDC_PROTOCOL_MIN_VERSION) */
    minVersion?: number;
    /** Maximum supported version (default: CDC_PROTOCOL_VERSION) */
    maxVersion?: number;
    /** Whether to validate before/after data presence (default: true) */
    validateData?: boolean;
    /** Maximum timestamp age in ms before warning (default: 1 hour) */
    maxTimestampAge?: number;
  } = {}
): CDCValidationResult {
  const {
    minVersion = CDC_PROTOCOL_MIN_VERSION,
    maxVersion = CDC_PROTOCOL_VERSION,
    validateData = true,
    maxTimestampAge = 3600000, // 1 hour
  } = options;

  const errors: CDCValidationError[] = [];
  const warnings: CDCValidationWarning[] = [];

  // Check if event is an object
  if (!event || typeof event !== 'object') {
    errors.push({
      code: CDCValidationErrorCode.MISSING_VERSION,
      message: 'Event must be an object',
    });
    return { valid: false, errors, warnings };
  }

  const e = event as Record<string, unknown>;

  // === Version Validation ===
  if (e.version === undefined || e.version === null) {
    errors.push({
      code: CDCValidationErrorCode.MISSING_VERSION,
      message: 'CDC event must have a version field',
      field: 'version',
    });
  } else if (typeof e.version !== 'number') {
    errors.push({
      code: CDCValidationErrorCode.UNSUPPORTED_VERSION,
      message: 'Version must be a number',
      field: 'version',
      expected: 'number',
      actual: typeof e.version,
    });
  } else if (e.version < minVersion || e.version > maxVersion) {
    errors.push({
      code: CDCValidationErrorCode.UNSUPPORTED_VERSION,
      message: `Version ${e.version} is not supported. Supported range: ${minVersion}-${maxVersion}`,
      field: 'version',
      expected: `${minVersion}-${maxVersion}`,
      actual: String(e.version),
    });
  }

  // === Type Validation ===
  if (e.type === undefined || e.type === null) {
    errors.push({
      code: CDCValidationErrorCode.MISSING_TYPE,
      message: 'CDC event must have a type field',
      field: 'type',
    });
  } else if (typeof e.type !== 'string') {
    errors.push({
      code: CDCValidationErrorCode.INVALID_TYPE,
      message: 'Type must be a string',
      field: 'type',
      expected: 'INSERT | UPDATE | DELETE',
      actual: typeof e.type,
    });
  } else if (!VALID_EVENT_TYPES.has(e.type as CDCEventType)) {
    errors.push({
      code: CDCValidationErrorCode.INVALID_TYPE,
      message: `Invalid event type: ${e.type}`,
      field: 'type',
      expected: 'INSERT | UPDATE | DELETE',
      actual: e.type as string,
    });
  }

  // === Table Validation ===
  if (e.table === undefined || e.table === null) {
    errors.push({
      code: CDCValidationErrorCode.MISSING_TABLE,
      message: 'CDC event must have a table field',
      field: 'table',
    });
  } else if (typeof e.table !== 'string') {
    errors.push({
      code: CDCValidationErrorCode.INVALID_TABLE,
      message: 'Table must be a string',
      field: 'table',
      expected: 'string',
      actual: typeof e.table,
    });
  } else if (e.table.trim().length === 0) {
    errors.push({
      code: CDCValidationErrorCode.INVALID_TABLE,
      message: 'Table name cannot be empty',
      field: 'table',
    });
  }

  // === LSN Validation ===
  if (e.lsn === undefined || e.lsn === null) {
    errors.push({
      code: CDCValidationErrorCode.MISSING_LSN,
      message: 'CDC event must have an lsn field',
      field: 'lsn',
    });
  } else if (typeof e.lsn !== 'bigint' && typeof e.lsn !== 'number') {
    errors.push({
      code: CDCValidationErrorCode.INVALID_LSN,
      message: 'LSN must be a bigint or number',
      field: 'lsn',
      expected: 'bigint | number',
      actual: typeof e.lsn,
    });
  }

  // === Timestamp Validation ===
  if (e.timestamp === undefined || e.timestamp === null) {
    errors.push({
      code: CDCValidationErrorCode.MISSING_TIMESTAMP,
      message: 'CDC event must have a timestamp field',
      field: 'timestamp',
    });
  } else if (typeof e.timestamp !== 'number') {
    errors.push({
      code: CDCValidationErrorCode.INVALID_TIMESTAMP,
      message: 'Timestamp must be a number (Unix timestamp in milliseconds)',
      field: 'timestamp',
      expected: 'number',
      actual: typeof e.timestamp,
    });
  } else {
    const now = Date.now();
    if (e.timestamp > now + 60000) {
      // More than 1 minute in the future
      warnings.push({
        code: CDCValidationWarningCode.FUTURE_TIMESTAMP,
        message: `Timestamp is ${e.timestamp - now}ms in the future`,
        field: 'timestamp',
      });
    } else if (now - e.timestamp > maxTimestampAge) {
      warnings.push({
        code: CDCValidationWarningCode.OLD_TIMESTAMP,
        message: `Timestamp is ${(now - e.timestamp) / 1000}s old`,
        field: 'timestamp',
      });
    }
  }

  // === Data Validation (conditional on event type) ===
  if (validateData && VALID_EVENT_TYPES.has(e.type as CDCEventType)) {
    switch (e.type) {
      case 'INSERT':
        if (e.after === undefined || e.after === null) {
          errors.push({
            code: CDCValidationErrorCode.INSERT_MISSING_AFTER,
            message: 'INSERT event must have after data',
            field: 'after',
          });
        }
        break;

      case 'UPDATE':
        if (e.before === undefined || e.before === null) {
          errors.push({
            code: CDCValidationErrorCode.UPDATE_MISSING_BEFORE,
            message: 'UPDATE event must have before data',
            field: 'before',
          });
        }
        if (e.after === undefined || e.after === null) {
          errors.push({
            code: CDCValidationErrorCode.UPDATE_MISSING_AFTER,
            message: 'UPDATE event must have after data',
            field: 'after',
          });
        }
        break;

      case 'DELETE':
        if (e.before === undefined || e.before === null) {
          errors.push({
            code: CDCValidationErrorCode.DELETE_MISSING_BEFORE,
            message: 'DELETE event must have before data',
            field: 'before',
          });
        }
        break;
    }
  }

  // === Optional Field Warnings ===
  if (e.schemaVersion === undefined) {
    warnings.push({
      code: CDCValidationWarningCode.MISSING_SCHEMA_VERSION,
      message: 'Schema version not specified',
      field: 'schemaVersion',
    });
  }

  if (e.primaryKey === undefined) {
    warnings.push({
      code: CDCValidationWarningCode.MISSING_PRIMARY_KEY,
      message: 'Primary key not specified',
      field: 'primaryKey',
    });
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * Type guard to check if an object is a valid VersionedCDCEvent.
 *
 * @param event - The object to check
 * @returns True if the object is a valid VersionedCDCEvent
 *
 * @example
 * ```typescript
 * const data: unknown = JSON.parse(message);
 * if (isVersionedCDCEvent(data)) {
 *   // data is typed as VersionedCDCEvent
 *   console.log(`Processing ${data.type} on ${data.table}`);
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function isVersionedCDCEvent(event: unknown): event is VersionedCDCEvent {
  const result = validateCDCEvent(event, { validateData: false });
  return result.valid;
}

/**
 * Negotiates the protocol version between producer and consumer capabilities.
 *
 * @param producer - Producer's capabilities
 * @param consumer - Consumer's capabilities
 * @returns Negotiation result with chosen version or error
 *
 * @example
 * ```typescript
 * const producer = DEFAULT_PRODUCER_CAPABILITIES;
 * const consumer = { ...DEFAULT_CONSUMER_CAPABILITIES, maxVersion: 2 };
 *
 * const result = negotiateProtocolVersion(producer, consumer);
 * if (result.success) {
 *   console.log(`Using protocol version ${result.negotiatedVersion}`);
 * }
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.4.0
 */
export function negotiateProtocolVersion(
  producer: CDCProtocolCapabilities,
  consumer: CDCProtocolCapabilities
): { success: boolean; negotiatedVersion: number; error?: string } {
  // Find the highest version both sides support
  const minShared = Math.max(producer.minVersion, consumer.minVersion);
  const maxShared = Math.min(producer.maxVersion, consumer.maxVersion);

  if (minShared > maxShared) {
    return {
      success: false,
      negotiatedVersion: 0,
      error: `No compatible protocol version. Producer: ${producer.minVersion}-${producer.maxVersion}, Consumer: ${consumer.minVersion}-${consumer.maxVersion}`,
    };
  }

  return {
    success: true,
    negotiatedVersion: maxShared,
  };
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Creates a new VersionedCDCEvent with the current protocol version.
 *
 * This is a convenience function for producers to create properly versioned events.
 *
 * @param event - Event data without version field
 * @returns Complete VersionedCDCEvent with version field
 *
 * @example
 * ```typescript
 * const event = createVersionedCDCEvent({
 *   type: 'INSERT',
 *   table: 'users',
 *   lsn: 1001n,
 *   timestamp: Date.now(),
 *   after: { id: 1, name: 'Alice' },
 * });
 *
 * // event.version is automatically set to CDC_PROTOCOL_VERSION
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function createVersionedCDCEvent<T = Row>(
  event: Omit<VersionedCDCEvent<T>, 'version'>
): VersionedCDCEvent<T> {
  return {
    version: CDC_PROTOCOL_VERSION,
    ...event,
  };
}

/**
 * Converts a legacy CDCEvent (without version) to a VersionedCDCEvent.
 *
 * Use this when receiving events from older producers that don't include
 * the version field.
 *
 * @param legacyEvent - Event without version field
 * @param assumedVersion - Version to assign (default: CDC_PROTOCOL_VERSION)
 * @returns VersionedCDCEvent with version field
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function upgradeToVersionedCDCEvent<T = Row>(
  legacyEvent: {
    table: string;
    lsn: bigint | number;
    timestamp: number | Date;
    operation?: string;
    type?: string;
    before?: T;
    after?: T;
    transactionId?: string;
    txId?: string;
    primaryKey?: Record<string, unknown>;
    metadata?: Record<string, unknown>;
  },
  assumedVersion: number = CDC_PROTOCOL_VERSION
): VersionedCDCEvent<T> {
  // Normalize operation/type field
  const type = (legacyEvent.type || legacyEvent.operation || 'INSERT').toUpperCase() as CDCEventType;

  // Normalize timestamp
  const timestamp = legacyEvent.timestamp instanceof Date
    ? legacyEvent.timestamp.getTime()
    : legacyEvent.timestamp;

  // Normalize LSN
  const lsn = typeof legacyEvent.lsn === 'number'
    ? BigInt(legacyEvent.lsn)
    : legacyEvent.lsn;

  // Normalize transaction ID
  const transactionId = legacyEvent.transactionId || legacyEvent.txId;

  // Build the result object, only including optional fields if they have values
  const result: VersionedCDCEvent<T> = {
    version: assumedVersion,
    type,
    table: legacyEvent.table,
    lsn,
    timestamp,
  };

  if (legacyEvent.before !== undefined) {
    result.before = legacyEvent.before;
  }
  if (legacyEvent.after !== undefined) {
    result.after = legacyEvent.after;
  }
  if (transactionId !== undefined) {
    result.transactionId = transactionId;
  }
  if (legacyEvent.primaryKey !== undefined) {
    result.primaryKey = legacyEvent.primaryKey;
  }
  if (legacyEvent.metadata !== undefined) {
    result.metadata = legacyEvent.metadata;
  }

  return result;
}
