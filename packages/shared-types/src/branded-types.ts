/**
 * Consolidated Branded Types for DoSQL Ecosystem
 *
 * This module provides additional branded types that are commonly used
 * across dosql and dolake packages. These types use the branded types pattern
 * for type-safe identifiers.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.3.0
 */

import { _isDevModeInternal, _isStrictModeInternal } from './config.js';

// =============================================================================
// Number-Based Branded Type Factory
// =============================================================================

/**
 * Creates a branded type factory function for number-based branded types with validation.
 *
 * This higher-order function generates type-safe factory functions for creating
 * branded number types. In development mode, the factory validates constraints.
 *
 * @typeParam Brand - The unique symbol type used for branding
 * @typeParam T - The resulting branded type (number & { readonly [Brand]: never })
 *
 * @param typeName - Human-readable name for error messages (e.g., "PageId")
 * @param options - Optional configuration for validation
 * @returns A factory function that converts numbers to the branded type
 *
 * @example
 * ```typescript
 * // Define a branded type
 * declare const PageIdBrand: unique symbol;
 * type PageId = number & { readonly [typeof PageIdBrand]: never };
 *
 * // Create the factory function
 * const createPageId = createBrandedNumberFactory<typeof PageIdBrand, PageId>(
 *   'PageId',
 *   { allowNegative: false, requireInteger: true }
 * );
 *
 * // Use it
 * const id = createPageId(42); // Type: PageId
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function createBrandedNumberFactory<
  Brand extends symbol,
  T extends number & { readonly [K in Brand]: never }
>(
  typeName: string,
  options: { allowZero?: boolean; allowNegative?: boolean; requireInteger?: boolean } = {}
): (value: number) => T {
  const { allowZero = true, allowNegative = false, requireInteger = false } = options;
  return (value: number): T => {
    if (_isDevModeInternal() || _isStrictModeInternal()) {
      if (typeof value !== 'number') {
        throw new Error(`${typeName} must be a number`);
      }
      if (!allowNegative && value < 0) {
        throw new Error(`${typeName} cannot be negative: ${value}`);
      }
      if (!allowZero && value === 0) {
        throw new Error(`${typeName} cannot be zero`);
      }
      if (requireInteger && !Number.isInteger(value)) {
        throw new Error(`${typeName} must be an integer: ${value}`);
      }
    }
    return value as T;
  };
}

/**
 * Creates a type guard function for a number-based branded type.
 *
 * Generates a type guard that checks if a value is a valid candidate
 * for the branded type.
 *
 * @typeParam T - The branded type to guard for
 *
 * @param _typeName - Human-readable name for the type (used for documentation)
 * @param options - Optional configuration for validation
 * @returns A type guard function that narrows unknown to T
 *
 * @example
 * ```typescript
 * const isPageId = createBrandedNumberGuard<PageId>('PageId', { requireInteger: true });
 *
 * const value: unknown = 42;
 * if (isPageId(value)) {
 *   // value is narrowed to PageId
 *   processPage(value);
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function createBrandedNumberGuard<T extends number>(
  _typeName: string,
  options: { allowZero?: boolean; allowNegative?: boolean; requireInteger?: boolean } = {}
): (value: unknown) => value is T {
  const { allowZero = true, allowNegative = false, requireInteger = false } = options;
  return (value: unknown): value is T => {
    if (typeof value !== 'number') return false;
    if (!allowNegative && value < 0) return false;
    if (!allowZero && value === 0) return false;
    if (requireInteger && !Number.isInteger(value)) return false;
    return true;
  };
}

// =============================================================================
// PageId - Number-based branded type for B-tree page identifiers
// =============================================================================

/** Brand symbol for Page ID */
declare const PageIdBrand: unique symbol;

/**
 * Page ID - A branded number type for B-tree page identifiers.
 *
 * Page IDs identify a specific page in the B-tree storage.
 * They must be non-negative integers.
 *
 * @example
 * ```typescript
 * import { createPageId, PageId } from '@dotdo/sql-types';
 *
 * const pageId: PageId = createPageId(42);
 * // pageId is PageId, not assignable from plain number
 *
 * // Error: Type 'number' is not assignable to type 'PageId'
 * // const invalid: PageId = 42;
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export type PageId = number & { readonly [PageIdBrand]: never };

/**
 * Create a branded PageId from a number value.
 *
 * This is the only safe way to create a PageId.
 *
 * @param value - The number value for the page ID
 * @returns A branded PageId value
 * @throws {Error} If value is negative or not an integer (in dev mode)
 *
 * @example
 * ```typescript
 * const pageId = createPageId(0);   // Valid: root page
 * const page2 = createPageId(100);  // Valid: page 100
 *
 * createPageId(-1);   // Error: PageId cannot be negative
 * createPageId(1.5);  // Error: PageId must be an integer
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export const createPageId = createBrandedNumberFactory<typeof PageIdBrand, PageId>(
  'PageId',
  { allowNegative: false, requireInteger: true }
);

/**
 * Type guard to check if a value is a valid PageId candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a non-negative integer
 *
 * @example
 * ```typescript
 * if (isValidPageId(value)) {
 *   const pageId = createPageId(value);
 *   // value is known to be a valid page ID
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export const isValidPageId = createBrandedNumberGuard<PageId>(
  'PageId',
  { allowNegative: false, requireInteger: true }
);

// =============================================================================
// SchemaVersion - Bigint-based branded type for schema versioning
// =============================================================================

/** Brand symbol for Schema Version */
declare const SchemaVersionBrand: unique symbol;

/**
 * Schema Version - A branded bigint type for schema version identifiers.
 *
 * SchemaVersion represents the version of a table schema, used for:
 * - CDC event enrichment with schema metadata
 * - Schema compatibility validation
 * - Schema evolution tracking in DoLake
 *
 * Schema versions are monotonically increasing numbers that track schema evolution.
 *
 * @example
 * ```typescript
 * import { createSchemaVersion, SchemaVersion } from '@dotdo/sql-types';
 *
 * const version: SchemaVersion = createSchemaVersion(1);
 *
 * // Use in schema tracking
 * const schema = {
 *   tableName: 'users',
 *   version: version,
 *   // ...
 * };
 *
 * // Increment for schema changes
 * const nextVersion = createSchemaVersion(version + 1n);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export type SchemaVersion = bigint & { readonly [SchemaVersionBrand]: never };

/**
 * Create a branded SchemaVersion from a bigint or number value.
 *
 * This is the recommended way to create SchemaVersion values.
 * Numbers are automatically converted to bigint.
 *
 * @param value - The version number as bigint or number
 * @returns A branded SchemaVersion value
 * @throws {Error} If value is negative (in dev mode)
 *
 * @example
 * ```typescript
 * // From number (common case)
 * const v1 = createSchemaVersion(1);
 *
 * // From bigint
 * const v2 = createSchemaVersion(2n);
 *
 * // Increment existing version
 * const v3 = createSchemaVersion(v2 + 1n);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function createSchemaVersion(value: bigint | number): SchemaVersion {
  const bigintValue = typeof value === 'bigint' ? value : BigInt(value);

  if (_isDevModeInternal() || _isStrictModeInternal()) {
    if (bigintValue < 0n) {
      throw new Error(`SchemaVersion cannot be negative: ${bigintValue}`);
    }
  }

  return bigintValue as SchemaVersion;
}

/**
 * Type guard to check if a value is a valid SchemaVersion candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a non-negative bigint
 *
 * @example
 * ```typescript
 * if (isValidSchemaVersion(value)) {
 *   const version = createSchemaVersion(value);
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function isValidSchemaVersion(value: unknown): value is bigint {
  return typeof value === 'bigint' && value >= 0n;
}

/**
 * Compare two schema versions.
 *
 * @param a - First schema version
 * @param b - Second schema version
 * @returns -1 if a < b, 0 if a === b, 1 if a > b
 *
 * @example
 * ```typescript
 * const v1 = createSchemaVersion(1);
 * const v2 = createSchemaVersion(2);
 *
 * compareSchemaVersion(v1, v2); // -1
 * compareSchemaVersion(v2, v1); // 1
 * compareSchemaVersion(v1, v1); // 0
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function compareSchemaVersion(a: SchemaVersion, b: SchemaVersion): -1 | 0 | 1 {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Increment a schema version by an amount.
 *
 * @param version - The schema version to increment
 * @param amount - The amount to increment by (default: 1)
 * @returns The incremented schema version
 *
 * @example
 * ```typescript
 * const v1 = createSchemaVersion(1);
 * const v2 = incrementSchemaVersion(v1); // 2n
 * const v5 = incrementSchemaVersion(v1, 4); // 5n
 * ```
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function incrementSchemaVersion(version: SchemaVersion, amount: bigint | number = 1n): SchemaVersion {
  const bigintAmount = typeof amount === 'bigint' ? amount : BigInt(amount);
  return createSchemaVersion(version + bigintAmount);
}

/**
 * Serialize a schema version to a string for JSON.
 *
 * @param version - The schema version to serialize
 * @returns The version as a string
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function serializeSchemaVersion(version: SchemaVersion): string {
  return version.toString();
}

/**
 * Deserialize a schema version from a string.
 *
 * @param value - The string value to deserialize
 * @returns The parsed SchemaVersion
 *
 * @public
 * @stability stable
 * @since 0.3.0
 */
export function deserializeSchemaVersion(value: string): SchemaVersion {
  return createSchemaVersion(BigInt(value));
}

// =============================================================================
// TableId - String-based branded type for table identifiers
// =============================================================================

/** Brand symbol for Table ID */
declare const TableIdBrand: unique symbol;

/**
 * Table ID - A branded string type for table identifiers.
 *
 * TableId uniquely identifies a table within a database.
 * Must be a non-empty string, max 255 characters.
 *
 * @example
 * ```typescript
 * import { createTableId, TableId } from '@dotdo/sql-types';
 *
 * const tableId: TableId = createTableId('users');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type TableId = string & { readonly [TableIdBrand]: never };

/**
 * Create a branded TableId from a string value.
 *
 * @param value - The string value for the table ID
 * @returns A branded TableId value
 * @throws {Error} If value is empty, whitespace-only, or exceeds max length (in dev mode)
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function createTableId(value: string): TableId {
  if (_isDevModeInternal() || _isStrictModeInternal()) {
    if (typeof value !== 'string') {
      throw new Error('TableId must be a string');
    }
    if (value.trim().length === 0) {
      throw new Error('TableId cannot be empty');
    }
    if (value.length > 255) {
      throw new Error('TableId exceeds maximum length of 255');
    }
  }
  return value as TableId;
}

/**
 * Type guard to check if a value is a valid TableId candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a non-empty string of at most 255 characters
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function isValidTableId(value: unknown): value is TableId {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 255;
}

// =============================================================================
// IndexId - String-based branded type for index identifiers
// =============================================================================

/** Brand symbol for Index ID */
declare const IndexIdBrand: unique symbol;

/**
 * Index ID - A branded string type for index identifiers.
 *
 * IndexId uniquely identifies an index within a table.
 * Must be a non-empty string, max 255 characters.
 *
 * @example
 * ```typescript
 * import { createIndexId, IndexId } from '@dotdo/sql-types';
 *
 * const indexId: IndexId = createIndexId('idx_users_email');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type IndexId = string & { readonly [IndexIdBrand]: never };

/**
 * Create a branded IndexId from a string value.
 *
 * @param value - The string value for the index ID
 * @returns A branded IndexId value
 * @throws {Error} If value is empty, whitespace-only, or exceeds max length (in dev mode)
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function createIndexId(value: string): IndexId {
  if (_isDevModeInternal() || _isStrictModeInternal()) {
    if (typeof value !== 'string') {
      throw new Error('IndexId must be a string');
    }
    if (value.trim().length === 0) {
      throw new Error('IndexId cannot be empty');
    }
    if (value.length > 255) {
      throw new Error('IndexId exceeds maximum length of 255');
    }
  }
  return value as IndexId;
}

/**
 * Type guard to check if a value is a valid IndexId candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a non-empty string of at most 255 characters
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function isValidIndexId(value: unknown): value is IndexId {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= 255;
}

// =============================================================================
// ColumnId - Number-based branded type for column identifiers
// =============================================================================

/** Brand symbol for Column ID */
declare const ColumnIdBrand: unique symbol;

/**
 * Column ID - A branded number type for column identifiers.
 *
 * ColumnId uniquely identifies a column within a table schema.
 * Must be a positive integer (zero is not allowed).
 *
 * @example
 * ```typescript
 * import { createColumnId, ColumnId } from '@dotdo/sql-types';
 *
 * const colId: ColumnId = createColumnId(1);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type ColumnId = number & { readonly [ColumnIdBrand]: never };

/**
 * Create a branded ColumnId from a number value.
 *
 * @param value - The number value for the column ID (must be positive integer)
 * @returns A branded ColumnId value
 * @throws {Error} If value is not a positive integer (in dev mode)
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const createColumnId = createBrandedNumberFactory<typeof ColumnIdBrand, ColumnId>(
  'ColumnId',
  { allowZero: false, allowNegative: false, requireInteger: true }
);

/**
 * Type guard to check if a value is a valid ColumnId candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a positive integer
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const isValidColumnId = createBrandedNumberGuard<ColumnId>(
  'ColumnId',
  { allowZero: false, allowNegative: false, requireInteger: true }
);

// =============================================================================
// PageOffset - Number-based branded type for byte offsets within a page
// =============================================================================

/** Brand symbol for Page Offset */
declare const PageOffsetBrand: unique symbol;

/**
 * Page Offset - A branded number type for byte offsets within a B-tree page.
 *
 * PageOffset represents a byte position within a serialized page.
 * Must be a non-negative integer.
 *
 * @example
 * ```typescript
 * import { createPageOffset, PageOffset } from '@dotdo/sql-types';
 *
 * const offset: PageOffset = createPageOffset(32); // After header
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type PageOffset = number & { readonly [PageOffsetBrand]: never };

/**
 * Create a branded PageOffset from a number value.
 *
 * @param value - The number value for the page offset
 * @returns A branded PageOffset value
 * @throws {Error} If value is negative or not an integer (in dev mode)
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const createPageOffset = createBrandedNumberFactory<typeof PageOffsetBrand, PageOffset>(
  'PageOffset',
  { allowNegative: false, requireInteger: true }
);

/**
 * Type guard to check if a value is a valid PageOffset candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a non-negative integer
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const isValidPageOffset = createBrandedNumberGuard<PageOffset>(
  'PageOffset',
  { allowNegative: false, requireInteger: true }
);

// =============================================================================
// ByteOffset - Number-based branded type for generic byte offsets
// =============================================================================

/** Brand symbol for Byte Offset */
declare const ByteOffsetBrand: unique symbol;

/**
 * Byte Offset - A branded number type for generic byte offsets in binary data.
 *
 * ByteOffset represents a byte position within any binary buffer.
 * Must be a non-negative integer.
 *
 * @example
 * ```typescript
 * import { createByteOffset, ByteOffset } from '@dotdo/sql-types';
 *
 * const offset: ByteOffset = createByteOffset(1024);
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type ByteOffset = number & { readonly [ByteOffsetBrand]: never };

/**
 * Create a branded ByteOffset from a number value.
 *
 * @param value - The number value for the byte offset
 * @returns A branded ByteOffset value
 * @throws {Error} If value is negative or not an integer (in dev mode)
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const createByteOffset = createBrandedNumberFactory<typeof ByteOffsetBrand, ByteOffset>(
  'ByteOffset',
  { allowNegative: false, requireInteger: true }
);

/**
 * Type guard to check if a value is a valid ByteOffset candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a non-negative integer
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export const isValidByteOffset = createBrandedNumberGuard<ByteOffset>(
  'ByteOffset',
  { allowNegative: false, requireInteger: true }
);

// =============================================================================
// RequestId - String-based branded type for request identifiers
// =============================================================================

/** Brand symbol for Request ID */
declare const RequestIdBrand: unique symbol;

/**
 * Request ID - A branded string type for request identifiers.
 *
 * RequestId uniquely identifies an API or RPC request for tracing
 * and correlation. Must be a non-empty string.
 *
 * @example
 * ```typescript
 * import { createRequestId, RequestId } from '@dotdo/sql-types';
 *
 * const reqId: RequestId = createRequestId('req-abc-123');
 * ```
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export type RequestId = string & { readonly [RequestIdBrand]: never };

/**
 * Create a branded RequestId from a string value.
 *
 * @param value - The string value for the request ID
 * @returns A branded RequestId value
 * @throws {Error} If value is empty or whitespace-only (in dev mode)
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function createRequestId(value: string): RequestId {
  if (_isDevModeInternal() || _isStrictModeInternal()) {
    if (typeof value !== 'string') {
      throw new Error('RequestId must be a string');
    }
    if (value.trim().length === 0) {
      throw new Error('RequestId cannot be empty');
    }
  }
  return value as RequestId;
}

/**
 * Type guard to check if a value is a valid RequestId candidate.
 *
 * @param value - The value to check
 * @returns True if the value is a non-empty string
 *
 * @public
 * @stability stable
 * @since 0.4.0
 */
export function isValidRequestId(value: unknown): value is RequestId {
  return typeof value === 'string' && value.trim().length > 0;
}
