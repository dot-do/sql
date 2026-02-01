/**
 * Storage Config Bridge
 *
 * Bridges DoSQL's StorageConfig (per-table parquetFileSize) into DoLake's
 * ScalingConfig for Parquet file size control.
 *
 * Issue: sql-zo55 - Wire StorageConfig through DoLake Parquet writer
 *
 * DoSQL defines per-table storage configuration via `StorageConfig.parquetFileSize`.
 * This module provides the glue so that DoLake's ScalingConfig respects those
 * per-table overrides when writing Parquet files.
 *
 * Inheritance: DEFAULT_SCALING_CONFIG.maxParquetFileSize (512MB)
 *            -> globalParquetFileSize override
 *            -> per-table parquetFileSize override
 */

import {
  type ScalingConfig,
  DEFAULT_SCALING_CONFIG,
} from './scalability.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Per-table storage override for Parquet file size.
 */
export interface TableParquetConfig {
  /** Target Parquet file size in bytes for this table */
  parquetFileSize: number;
}

/**
 * Collection of per-table storage overrides.
 *
 * These flow from DoSQL's StorageConfig through the CDC pipeline.
 * Per-table config takes priority over globalParquetFileSize, which
 * takes priority over DEFAULT_SCALING_CONFIG.maxParquetFileSize.
 */
export interface TableStorageOverrides {
  /** Global parquetFileSize override (applies to all tables without per-table config) */
  globalParquetFileSize?: number;
  /** Per-table overrides keyed by table name */
  tables: Record<string, TableParquetConfig>;
}

// =============================================================================
// Functions
// =============================================================================

/**
 * Get the max Parquet file size for a given table.
 *
 * Resolution order:
 * 1. Per-table override from `overrides.tables[tableName].parquetFileSize`
 * 2. Global override from `overrides.globalParquetFileSize`
 * 3. Default from `DEFAULT_SCALING_CONFIG.maxParquetFileSize` (512MB)
 *
 * @param tableName - Name of the table
 * @param overrides - Optional per-table and global overrides
 * @returns The resolved max Parquet file size in bytes
 */
export function getMaxParquetFileSize(
  tableName: string,
  overrides?: TableStorageOverrides,
): number {
  if (overrides) {
    // Per-table override takes highest priority
    const tableConfig = overrides.tables[tableName];
    if (tableConfig?.parquetFileSize !== undefined) {
      return tableConfig.parquetFileSize;
    }

    // Global override is next
    if (overrides.globalParquetFileSize !== undefined) {
      return overrides.globalParquetFileSize;
    }
  }

  // Fall back to the default
  return DEFAULT_SCALING_CONFIG.maxParquetFileSize;
}

/**
 * Apply per-table storage config to a ScalingConfig, returning a new config
 * with maxParquetFileSize adjusted for the given table.
 *
 * All other ScalingConfig fields are preserved unchanged.
 *
 * @param baseConfig - The base ScalingConfig to start from
 * @param tableName - Name of the table being written
 * @param overrides - Optional per-table and global overrides
 * @returns A new ScalingConfig with maxParquetFileSize resolved for the table
 */
export function applyTableStorageConfig(
  baseConfig: ScalingConfig,
  tableName: string,
  overrides?: TableStorageOverrides,
): ScalingConfig {
  const resolvedSize = getMaxParquetFileSize(tableName, overrides);

  if (resolvedSize === baseConfig.maxParquetFileSize) {
    return baseConfig;
  }

  return {
    ...baseConfig,
    maxParquetFileSize: resolvedSize,
  };
}
