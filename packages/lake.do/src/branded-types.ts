/**
 * @dotdo/lake.do - Branded Type Factory Pattern
 *
 * This module provides a generic factory pattern for creating branded types,
 * eliminating repetitive validation logic across multiple branded type factories.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

import { isDevMode } from '@dotdo/sql.do';

// =============================================================================
// Branded Type Factory
// =============================================================================

/**
 * Creates a branded type factory function with validation.
 *
 * @description This higher-order function generates type-safe factory functions
 * for branded types. In development mode, the factory validates that the input
 * is a non-empty string. In production, validation is skipped for performance.
 *
 * @typeParam Brand - The unique symbol type used for branding
 * @typeParam T - The resulting branded type (string & { readonly [Brand]: never })
 *
 * @param typeName - Human-readable name for error messages (e.g., "CDCEventId")
 * @returns A factory function that converts strings to the branded type
 *
 * @example
 * ```typescript
 * // Define a branded type
 * declare const MyIdBrand: unique symbol;
 * type MyId = string & { readonly [typeof MyIdBrand]: never };
 *
 * // Create the factory function
 * const createMyId = createBrandedTypeFactory<typeof MyIdBrand, MyId>('MyId');
 *
 * // Use it
 * const id = createMyId('my-123'); // Type: MyId
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createBrandedTypeFactory<
  Brand extends symbol,
  T extends string & { readonly [K in Brand]: never }
>(typeName: string): (value: string) => T {
  return (value: string): T => {
    if (isDevMode()) {
      if (typeof value !== 'string') {
        throw new Error(`${typeName} must be a string`);
      }
      if (value.trim().length === 0) {
        throw new Error(`${typeName} cannot be empty`);
      }
    }
    return value as T;
  };
}

// =============================================================================
// Type Guard Factory
// =============================================================================

/**
 * Creates a type guard function for a branded type.
 *
 * @description Generates a type guard that checks if a value is a valid
 * candidate for the branded type (i.e., a non-empty string).
 *
 * @typeParam T - The branded type to guard for
 *
 * @param typeName - Human-readable name for the type (used for documentation)
 * @returns A type guard function that narrows unknown to T
 *
 * @example
 * ```typescript
 * const isMyId = createBrandedTypeGuard<MyId>('MyId');
 *
 * const value: unknown = 'my-123';
 * if (isMyId(value)) {
 *   // value is narrowed to MyId
 *   processId(value);
 * }
 * ```
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export function createBrandedTypeGuard<T extends string>(
  _typeName: string
): (value: unknown) => value is T {
  return (value: unknown): value is T => {
    return typeof value === 'string' && value.trim().length > 0;
  };
}

// =============================================================================
// Branded Types (Declarations)
// =============================================================================

declare const CDCEventIdBrand: unique symbol;
declare const PartitionKeyBrand: unique symbol;
declare const ParquetFileIdBrand: unique symbol;
declare const SnapshotIdBrand: unique symbol;
declare const CompactionJobIdBrand: unique symbol;

/**
 * Branded type for CDC event identifiers.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type CDCEventId = string & { readonly [CDCEventIdBrand]: never };

/**
 * Branded type for partition key identifiers.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type PartitionKey = string & { readonly [PartitionKeyBrand]: never };

/**
 * Branded type for Parquet file identifiers.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type ParquetFileId = string & { readonly [ParquetFileIdBrand]: never };

/**
 * Branded type for snapshot identifiers.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type SnapshotId = string & { readonly [SnapshotIdBrand]: never };

/**
 * Branded type for compaction job identifiers.
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export type CompactionJobId = string & { readonly [CompactionJobIdBrand]: never };

// =============================================================================
// Factory Functions (using the generic pattern)
// =============================================================================

/**
 * Creates a branded CDCEventId from a plain string.
 *
 * @param id - The plain string identifier for the CDC event
 * @returns A branded CDCEventId
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const createCDCEventId = createBrandedTypeFactory<typeof CDCEventIdBrand, CDCEventId>('CDCEventId');

/**
 * Creates a branded PartitionKey from a plain string.
 *
 * @param key - The plain string partition key
 * @returns A branded PartitionKey
 * @throws {Error} In dev mode: if key is not a string or is empty
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const createPartitionKey = createBrandedTypeFactory<typeof PartitionKeyBrand, PartitionKey>('PartitionKey');

/**
 * Creates a branded ParquetFileId from a plain string.
 *
 * @param id - The plain string file identifier
 * @returns A branded ParquetFileId
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const createParquetFileId = createBrandedTypeFactory<typeof ParquetFileIdBrand, ParquetFileId>('ParquetFileId');

/**
 * Creates a branded SnapshotId from a plain string.
 *
 * @param id - The plain string snapshot identifier
 * @returns A branded SnapshotId
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const createSnapshotId = createBrandedTypeFactory<typeof SnapshotIdBrand, SnapshotId>('SnapshotId');

/**
 * Creates a branded CompactionJobId from a plain string.
 *
 * @param id - The plain string job identifier
 * @returns A branded CompactionJobId
 * @throws {Error} In dev mode: if id is not a string or is empty
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const createCompactionJobId = createBrandedTypeFactory<typeof CompactionJobIdBrand, CompactionJobId>('CompactionJobId');

// =============================================================================
// Type Guards
// =============================================================================

/**
 * Type guard for CDCEventId.
 *
 * @param value - The value to check
 * @returns True if the value is a valid CDCEventId candidate
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const isCDCEventId = createBrandedTypeGuard<CDCEventId>('CDCEventId');

/**
 * Type guard for PartitionKey.
 *
 * @param value - The value to check
 * @returns True if the value is a valid PartitionKey candidate
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const isPartitionKey = createBrandedTypeGuard<PartitionKey>('PartitionKey');

/**
 * Type guard for ParquetFileId.
 *
 * @param value - The value to check
 * @returns True if the value is a valid ParquetFileId candidate
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const isParquetFileId = createBrandedTypeGuard<ParquetFileId>('ParquetFileId');

/**
 * Type guard for SnapshotId.
 *
 * @param value - The value to check
 * @returns True if the value is a valid SnapshotId candidate
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const isSnapshotId = createBrandedTypeGuard<SnapshotId>('SnapshotId');

/**
 * Type guard for CompactionJobId.
 *
 * @param value - The value to check
 * @returns True if the value is a valid CompactionJobId candidate
 *
 * @public
 * @stability stable
 * @since 0.1.0
 */
export const isCompactionJobId = createBrandedTypeGuard<CompactionJobId>('CompactionJobId');
