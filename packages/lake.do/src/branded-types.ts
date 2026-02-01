/**
 * lake.do - Branded Type Factory Pattern
 *
 * This module re-exports the generic factory pattern from @dotdo/sql-types
 * and defines lake.do-specific branded types using the standardized pattern.
 *
 * @packageDocumentation
 * @stability stable
 * @since 0.1.0
 */

// =============================================================================
// Import and Re-export Generic Factory Pattern from sql.do
// =============================================================================

import {
  createBrandedTypeFactory as _createBrandedTypeFactory,
  createBrandedBigintFactory as _createBrandedBigintFactory,
  createBrandedTypeGuard as _createBrandedTypeGuard,
  createBrandedBigintGuard as _createBrandedBigintGuard,
} from 'sql.do';

/**
 * Re-exported generic branded type factory functions from `sql.do` (which re-exports from `@dotdo/sql-types`).
 *
 * These factory functions provide the standardized pattern for creating
 * branded types across all packages in the DoSQL ecosystem.
 *
 * @see {@link https://github.com/dotdo/shared-types | @dotdo/sql-types} for canonical definitions
 *
 * @public
 * @stability stable
 * @since 0.2.0
 */
export const createBrandedTypeFactory = _createBrandedTypeFactory;
export const createBrandedBigintFactory = _createBrandedBigintFactory;
export const createBrandedTypeGuard = _createBrandedTypeGuard;
export const createBrandedBigintGuard = _createBrandedBigintGuard;

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
