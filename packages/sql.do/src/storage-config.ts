/**
 * Storage Configuration Types for sql.do SDK
 *
 * Provides TypeScript type definitions and utilities for per-table and
 * per-database storage settings used with the WITH STORAGE clause in
 * CREATE TABLE statements.
 *
 * These types mirror the server-side StorageConfig in dosql, giving
 * SDK users type safety and autocompletion when configuring storage.
 *
 * @example
 * ```typescript
 * import type { TableStorageConfig } from 'sql.do';
 * import { DEFAULT_STORAGE_CONFIG, isValidStorageConfig } from 'sql.do';
 *
 * // Type-safe storage config for a table
 * const config: TableStorageConfig = {
 *   rowGroupSize: 4 * 1024 * 1024,       // 4MB row groups
 *   parquetFileSize: 256 * 1024 * 1024,   // 256MB Parquet files
 * };
 *
 * // Use WITH STORAGE clause in CREATE TABLE
 * await client.exec(`
 *   CREATE TABLE events (id INTEGER PRIMARY KEY, data TEXT)
 *   WITH STORAGE (rowGroupSize = '4MB', parquetFileSize = '256MB')
 * `);
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Complete storage configuration with all settings resolved.
 * Every field is required (no undefined values).
 *
 * | Setting            | Default | Range          | Description                          |
 * |--------------------|---------|----------------|--------------------------------------|
 * | chunkSize          | 2MB     | 1KB - 2MB      | DO storage chunk size                |
 * | maxPageSize        | 2MB     | 4KB - 2MB      | B-tree page size limit               |
 * | rowGroupSize       | 1MB     | 64KB - 128MB   | Columnar row group target size       |
 * | maxRowsPerRowGroup | 65536   | 1 - 1048576    | Max rows per row group               |
 * | hotStorageMaxSize  | 100MB   | 1MB - 1GB      | Max hot (DO) storage per table       |
 * | hotDataMaxAge      | 1h      | 1m - 30d       | Max age before migration to cold     |
 * | maxHotFileSize     | 10MB    | 1KB - 100MB    | Largest file kept in hot storage     |
 * | parquetFileSize    | 512MB   | 1MB - 5GB      | Target Parquet file size on R2       |
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export interface StorageConfig {
  /** DO storage chunk size in bytes (max 2MB - DO storage limit) */
  chunkSize: number;
  /** B-tree page size limit in bytes (max 2MB) */
  maxPageSize: number;
  /** Target columnar row group size in bytes */
  rowGroupSize: number;
  /** Maximum rows per row group */
  maxRowsPerRowGroup: number;
  /** Maximum hot (DO) storage in bytes */
  hotStorageMaxSize: number;
  /** Maximum age of hot data in milliseconds before migration */
  hotDataMaxAge: number;
  /** Maximum individual file size to keep in hot storage */
  maxHotFileSize: number;
  /** Target Parquet file size in bytes for DoLake */
  parquetFileSize: number;
}

/**
 * Partial storage configuration for database-level defaults.
 * Any setting not specified falls back to DEFAULT_STORAGE_CONFIG.
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export type DatabaseStorageConfig = Partial<StorageConfig>;

/**
 * Partial storage configuration for table-level overrides.
 * Any setting not specified falls back to database-level, then defaults.
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export type TableStorageConfig = Partial<StorageConfig>;

/**
 * Valid field names for storage configuration.
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export type StorageConfigField = keyof StorageConfig;

// =============================================================================
// Constants
// =============================================================================

const KB = 1024;
const MB = 1024 * 1024;

/**
 * All valid storage configuration field names.
 *
 * Useful for validation, iteration, and building UIs that configure storage.
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export const STORAGE_CONFIG_FIELDS: readonly StorageConfigField[] = Object.freeze([
  'chunkSize',
  'maxPageSize',
  'rowGroupSize',
  'maxRowsPerRowGroup',
  'hotStorageMaxSize',
  'hotDataMaxAge',
  'maxHotFileSize',
  'parquetFileSize',
] as const);

/** Set for O(1) lookup */
const VALID_FIELDS = new Set<string>(STORAGE_CONFIG_FIELDS);

/**
 * Default storage configuration matching the server-side defaults.
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export const DEFAULT_STORAGE_CONFIG: Readonly<StorageConfig> = Object.freeze({
  chunkSize: 2 * MB,
  maxPageSize: 2 * MB,
  rowGroupSize: 1 * MB,
  maxRowsPerRowGroup: 65536,
  hotStorageMaxSize: 100 * MB,
  hotDataMaxAge: 60 * 60 * 1000, // 1 hour
  maxHotFileSize: 10 * MB,
  parquetFileSize: 512 * MB,
});

// =============================================================================
// Validation
// =============================================================================

/**
 * Validates that a partial storage config object contains only known fields
 * with valid numeric (non-negative) values.
 *
 * This is a client-side check for catching configuration errors early.
 * Full range validation is performed server-side.
 *
 * @param config - The configuration object to validate
 * @returns `true` if all fields are valid storage config fields with non-negative numbers
 *
 * @example
 * ```typescript
 * import { isValidStorageConfig } from 'sql.do';
 *
 * isValidStorageConfig({ rowGroupSize: 4194304 });          // true
 * isValidStorageConfig({});                                  // true
 * isValidStorageConfig({ unknownField: 123 });               // false
 * isValidStorageConfig({ chunkSize: -1 });                   // false
 * isValidStorageConfig({ chunkSize: 'big' });                // false
 * ```
 *
 * @public
 * @stability experimental
 * @since 0.2.0
 */
export function isValidStorageConfig(config: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(config)) {
    if (!VALID_FIELDS.has(key)) {
      return false;
    }
    if (typeof value !== 'number' || value < 0) {
      return false;
    }
  }
  return true;
}
