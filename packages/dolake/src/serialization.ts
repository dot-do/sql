/**
 * BigInt Serialization for Iceberg Types
 *
 * Provides custom JSON serialization/deserialization for Iceberg metadata
 * that contains bigint values (snapshot IDs, sequence numbers, timestamps, etc.)
 *
 * Standard JSON does not support bigint, so we:
 * - Convert bigint to string on serialization (with type marker)
 * - Convert marked strings back to bigint on deserialization
 *
 * @example
 * ```typescript
 * import { serialize, deserialize } from './serialization.js';
 *
 * const snapshot = {
 *   'snapshot-id': 1234567890123456789n,
 *   'sequence-number': 1n,
 * };
 *
 * const json = serialize(snapshot);
 * // '{"snapshot-id":"__bigint__:1234567890123456789","sequence-number":"__bigint__:1"}'
 *
 * const restored = deserialize<typeof snapshot>(json);
 * // { 'snapshot-id': 1234567890123456789n, 'sequence-number': 1n }
 * ```
 */

// =============================================================================
// Constants
// =============================================================================

/**
 * Marker prefix for bigint values in serialized JSON.
 * Using a prefix that is unlikely to appear in normal string data.
 */
export const BIGINT_MARKER = '__bigint__:';

/**
 * Known Iceberg fields that should always be treated as bigint.
 * This provides a fallback for deserialization of numeric strings
 * that don't have the explicit marker (e.g., from external systems).
 */
export const ICEBERG_BIGINT_FIELDS = new Set([
  'snapshot-id',
  'parent-snapshot-id',
  'sequence-number',
  'timestamp-ms',
  'last-sequence-number',
  'last-updated-ms',
  'current-snapshot-id',
  'manifest-length',
  'min-sequence-number',
  'added-snapshot-id',
  'added-rows-count',
  'existing-rows-count',
  'deleted-rows-count',
  'record-count',
  'file-size-in-bytes',
]);

// =============================================================================
// Branded Types
// =============================================================================

/**
 * Branded type for snapshot IDs.
 * Helps ensure type safety when working with snapshot identifiers.
 */
export type SnapshotId = bigint & { readonly __brand: 'SnapshotId' };

/**
 * Branded type for sequence numbers.
 */
export type SequenceNumber = bigint & { readonly __brand: 'SequenceNumber' };

/**
 * Branded type for timestamps in milliseconds.
 */
export type TimestampMs = bigint & { readonly __brand: 'TimestampMs' };

/**
 * Create a branded SnapshotId from a bigint.
 */
export function snapshotId(value: bigint): SnapshotId {
  return value as SnapshotId;
}

/**
 * Create a branded SequenceNumber from a bigint.
 */
export function sequenceNumber(value: bigint): SequenceNumber {
  return value as SequenceNumber;
}

/**
 * Create a branded TimestampMs from a bigint.
 */
export function timestampMs(value: bigint): TimestampMs {
  return value as TimestampMs;
}

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Check if a value is a bigint.
 */
export function isBigInt(value: unknown): value is bigint {
  return typeof value === 'bigint';
}

/**
 * Check if a string is a serialized bigint (has marker prefix).
 */
export function isSerializedBigInt(value: unknown): value is string {
  return typeof value === 'string' && value.startsWith(BIGINT_MARKER);
}

/**
 * Check if a string looks like a large numeric value that should be bigint.
 * This is used for values without explicit markers (from external systems).
 */
export function isNumericBigIntString(value: unknown): value is string {
  if (typeof value !== 'string') return false;
  // Match strings of 15+ digits (beyond safe integer range)
  return /^\d{15,}$/.test(value);
}

/**
 * Check if a field name is a known Iceberg bigint field.
 */
export function isIcebergBigIntField(fieldName: string): boolean {
  return ICEBERG_BIGINT_FIELDS.has(fieldName);
}

// =============================================================================
// Serialization Functions
// =============================================================================

/**
 * JSON replacer function that converts bigint values to marked strings.
 *
 * @param _key - The key of the property being serialized
 * @param value - The value being serialized
 * @returns The transformed value (bigint -> marked string, others unchanged)
 *
 * @example
 * ```typescript
 * JSON.stringify({ id: 123n }, bigintReplacer);
 * // '{"id":"__bigint__:123"}'
 * ```
 */
export function bigintReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') {
    return `${BIGINT_MARKER}${value.toString()}`;
  }
  return value;
}

/**
 * JSON reviver function that converts marked strings back to bigint.
 *
 * @param key - The key of the property being deserialized
 * @param value - The value being deserialized
 * @returns The transformed value (marked string -> bigint, others unchanged)
 *
 * @example
 * ```typescript
 * JSON.parse('{"id":"__bigint__:123"}', bigintReviver);
 * // { id: 123n }
 * ```
 */
export function bigintReviver(key: string, value: unknown): unknown {
  // Handle explicitly marked bigint strings
  if (isSerializedBigInt(value)) {
    return BigInt(value.slice(BIGINT_MARKER.length));
  }

  // Handle numeric strings for known Iceberg bigint fields (any size)
  if (isIcebergBigIntField(key) && typeof value === 'string' && /^\d+$/.test(value)) {
    return BigInt(value);
  }

  // Handle large numeric strings (15+ digits) for any field
  if (isNumericBigIntString(value)) {
    return BigInt(value);
  }

  return value;
}

/**
 * Create a custom replacer that combines bigint handling with additional logic.
 *
 * @param customReplacer - Additional replacer function to chain
 * @returns Combined replacer function
 */
export function createReplacer(
  customReplacer?: (key: string, value: unknown) => unknown
): (key: string, value: unknown) => unknown {
  return (key: string, value: unknown): unknown => {
    // First apply bigint conversion
    const converted = bigintReplacer(key, value);
    // Then apply custom logic if provided
    return customReplacer ? customReplacer(key, converted) : converted;
  };
}

/**
 * Create a custom reviver that combines bigint handling with additional logic.
 *
 * @param customReviver - Additional reviver function to chain
 * @returns Combined reviver function
 */
export function createReviver(
  customReviver?: (key: string, value: unknown) => unknown
): (key: string, value: unknown) => unknown {
  return (key: string, value: unknown): unknown => {
    // First apply bigint conversion
    const converted = bigintReviver(key, value);
    // Then apply custom logic if provided
    return customReviver ? customReviver(key, converted) : converted;
  };
}

// =============================================================================
// High-Level API
// =============================================================================

/**
 * Serialize an object to JSON string with bigint support.
 *
 * @param value - The value to serialize
 * @param space - Optional indentation for pretty printing
 * @returns JSON string with bigint values converted to marked strings
 *
 * @example
 * ```typescript
 * const snapshot = { 'snapshot-id': 1234567890123456789n };
 * const json = serialize(snapshot);
 * // '{"snapshot-id":"__bigint__:1234567890123456789"}'
 * ```
 */
export function serialize(value: unknown, space?: number | string): string {
  return JSON.stringify(value, bigintReplacer, space);
}

/**
 * Deserialize a JSON string with bigint support.
 *
 * @param text - The JSON string to parse
 * @returns Parsed object with marked strings converted back to bigint
 *
 * @example
 * ```typescript
 * const json = '{"snapshot-id":"__bigint__:1234567890123456789"}';
 * const snapshot = deserialize<{ 'snapshot-id': bigint }>(json);
 * // { 'snapshot-id': 1234567890123456789n }
 * ```
 */
export function deserialize<T>(text: string): T {
  return JSON.parse(text, bigintReviver) as T;
}

// =============================================================================
// WebSocket Message Helpers
// =============================================================================

/**
 * Serialize a WebSocket message for sending.
 * Handles bigint values in the message payload.
 *
 * @param message - The message object to serialize
 * @returns JSON string ready for WebSocket transmission
 */
export function serializeMessage(message: unknown): string {
  return serialize(message);
}

/**
 * Deserialize a received WebSocket message.
 * Converts marked strings back to bigint values.
 *
 * @param data - The raw message data (string or ArrayBuffer)
 * @returns Parsed message object with bigint values restored
 */
export function deserializeMessage<T>(data: string | ArrayBuffer): T {
  const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
  return deserialize<T>(text);
}

// =============================================================================
// Iceberg-Specific Helpers
// =============================================================================

/**
 * Options for Iceberg serialization.
 */
export interface IcebergSerializationOptions {
  /**
   * Whether to use explicit markers for bigint values.
   * If false, bigints are converted to plain numeric strings.
   * Default: true
   */
  useMarkers?: boolean;

  /**
   * Whether to pretty-print the JSON output.
   * Default: false
   */
  pretty?: boolean;
}

/**
 * Serialize Iceberg metadata to JSON.
 * Uses plain numeric strings (without markers) for compatibility with
 * external Iceberg readers.
 *
 * @param metadata - The Iceberg metadata object
 * @param options - Serialization options
 * @returns JSON string with bigint values as numeric strings
 */
export function serializeIcebergMetadata(
  metadata: unknown,
  options: IcebergSerializationOptions = {}
): string {
  const { useMarkers = false, pretty = false } = options;

  const replacer = useMarkers
    ? bigintReplacer
    : (_key: string, value: unknown): unknown =>
        typeof value === 'bigint' ? value.toString() : value;

  return JSON.stringify(metadata, replacer, pretty ? 2 : undefined);
}

/**
 * Deserialize Iceberg metadata from JSON.
 * Automatically converts numeric strings in known bigint fields to bigint.
 *
 * @param json - The JSON string to parse
 * @returns Parsed Iceberg metadata with bigint values restored
 */
export function deserializeIcebergMetadata<T>(json: string): T {
  return JSON.parse(json, bigintReviver) as T;
}

// =============================================================================
// Validation Helpers
// =============================================================================

/**
 * Validate that a value is a valid snapshot ID.
 * Snapshot IDs must be positive bigint values.
 *
 * @param value - The value to validate
 * @returns True if the value is a valid snapshot ID
 */
export function isValidSnapshotId(value: unknown): value is bigint {
  return typeof value === 'bigint' && value > 0n;
}

/**
 * Validate that a value is a valid sequence number.
 * Sequence numbers must be non-negative bigint values.
 *
 * @param value - The value to validate
 * @returns True if the value is a valid sequence number
 */
export function isValidSequenceNumber(value: unknown): value is bigint {
  return typeof value === 'bigint' && value >= 0n;
}

/**
 * Validate that a value is a valid timestamp.
 * Timestamps must be positive bigint values representing milliseconds since epoch.
 *
 * @param value - The value to validate
 * @returns True if the value is a valid timestamp
 */
export function isValidTimestamp(value: unknown): value is bigint {
  return typeof value === 'bigint' && value > 0n;
}

/**
 * Parse a snapshot ID from various input types.
 * Accepts bigint, number (for small values), or string.
 *
 * @param value - The value to parse
 * @returns The parsed bigint value
 * @throws Error if the value cannot be parsed as a valid snapshot ID
 */
export function parseSnapshotId(value: bigint | number | string): bigint {
  if (typeof value === 'bigint') {
    return value;
  }
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`Invalid snapshot ID: ${value}`);
    }
    return BigInt(value);
  }
  if (typeof value === 'string') {
    // Remove marker prefix if present
    const numStr = value.startsWith(BIGINT_MARKER)
      ? value.slice(BIGINT_MARKER.length)
      : value;
    const parsed = BigInt(numStr);
    if (parsed <= 0n) {
      throw new Error(`Invalid snapshot ID: ${value}`);
    }
    return parsed;
  }
  throw new Error(`Cannot parse snapshot ID from ${typeof value}`);
}

/**
 * Parse a sequence number from various input types.
 *
 * @param value - The value to parse
 * @returns The parsed bigint value
 * @throws Error if the value cannot be parsed as a valid sequence number
 */
export function parseSequenceNumber(value: bigint | number | string): bigint {
  if (typeof value === 'bigint') {
    return value;
  }
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`Invalid sequence number: ${value}`);
    }
    return BigInt(value);
  }
  if (typeof value === 'string') {
    const numStr = value.startsWith(BIGINT_MARKER)
      ? value.slice(BIGINT_MARKER.length)
      : value;
    const parsed = BigInt(numStr);
    if (parsed < 0n) {
      throw new Error(`Invalid sequence number: ${value}`);
    }
    return parsed;
  }
  throw new Error(`Cannot parse sequence number from ${typeof value}`);
}
