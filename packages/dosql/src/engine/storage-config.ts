/**
 * Storage Configuration
 *
 * Per-table and per-database storage settings that control chunk sizes,
 * row group sizes, tiered storage thresholds, and Parquet file sizes.
 *
 * Inheritance: DEFAULT_STORAGE_CONFIG → DatabaseStorageConfig → TableStorageConfig
 *
 * ## Settings
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
 */

// =============================================================================
// Types
// =============================================================================

/**
 * Complete storage configuration with all settings resolved.
 * Every field is required (no undefined values).
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
 */
export type DatabaseStorageConfig = Partial<StorageConfig>;

/**
 * Partial storage configuration for table-level overrides.
 * Any setting not specified falls back to database-level, then defaults.
 */
export type TableStorageConfig = Partial<StorageConfig>;

/**
 * Validation result for storage configuration.
 */
export interface StorageConfigValidation {
  valid: boolean;
  errors: StorageConfigError[];
}

export interface StorageConfigError {
  field: keyof StorageConfig;
  message: string;
  value: number;
  min?: number;
  max?: number;
}

// =============================================================================
// Constants
// =============================================================================

const KB = 1024;
const MB = 1024 * 1024;
const GB = 1024 * 1024 * 1024;

/** DO storage value limit */
const DO_STORAGE_LIMIT = 2 * MB;

/** R2 single object limit */
const R2_SINGLE_OBJECT_LIMIT = 5 * GB;

/**
 * Default storage configuration matching current hardcoded values.
 */
export const DEFAULT_STORAGE_CONFIG: Readonly<StorageConfig> = Object.freeze({
  chunkSize: 2 * MB,            // DO storage limit
  maxPageSize: 2 * MB,          // B-tree page limit
  rowGroupSize: 1 * MB,         // Columnar target
  maxRowsPerRowGroup: 65536,    // Columnar max rows
  hotStorageMaxSize: 100 * MB,  // DO hot tier
  hotDataMaxAge: 60 * 60 * 1000, // 1 hour
  maxHotFileSize: 10 * MB,      // Bypass hot for large files
  parquetFileSize: 512 * MB,    // DoLake Parquet target
});

// =============================================================================
// Merge Logic
// =============================================================================

/**
 * Merge storage configurations with inheritance:
 * DEFAULT_STORAGE_CONFIG → dbConfig → tableConfig
 *
 * Undefined values in overrides are skipped (fall through to lower level).
 */
export function mergeStorageConfig(
  dbConfig?: DatabaseStorageConfig,
  tableConfig?: TableStorageConfig,
): StorageConfig {
  const result = { ...DEFAULT_STORAGE_CONFIG };

  // Apply database-level overrides
  if (dbConfig) {
    for (const key of Object.keys(dbConfig) as Array<keyof StorageConfig>) {
      const value = dbConfig[key];
      if (value !== undefined) {
        result[key] = value;
      }
    }
  }

  // Apply table-level overrides
  if (tableConfig) {
    for (const key of Object.keys(tableConfig) as Array<keyof StorageConfig>) {
      const value = tableConfig[key];
      if (value !== undefined) {
        result[key] = value;
      }
    }
  }

  return result;
}

// =============================================================================
// Validation
// =============================================================================

/**
 * Validate a resolved storage configuration.
 * Returns validation result with specific errors.
 */
export function validateStorageConfig(config: StorageConfig): StorageConfigValidation {
  const errors: StorageConfigError[] = [];

  const check = (
    field: keyof StorageConfig,
    min: number,
    max: number,
    label?: string,
  ) => {
    const value = config[field];
    if (value < min || value > max) {
      errors.push({
        field,
        message: `${label || field} must be between ${formatStorageSize(min)} and ${formatStorageSize(max)}, got ${formatStorageSize(value)}`,
        value,
        min,
        max,
      });
    }
  };

  // Validate each setting against its constraints
  check('chunkSize', 1 * KB, DO_STORAGE_LIMIT, 'Chunk size');
  check('maxPageSize', 4 * KB, DO_STORAGE_LIMIT, 'Max page size');
  check('rowGroupSize', 64 * KB, 128 * MB, 'Row group size');
  check('maxRowsPerRowGroup', 1, 1048576, 'Max rows per row group');
  check('hotStorageMaxSize', 1 * MB, 1 * GB, 'Hot storage max size');
  check('hotDataMaxAge', 60 * 1000, 30 * 24 * 60 * 60 * 1000, 'Hot data max age');
  check('maxHotFileSize', 1 * KB, 100 * MB, 'Max hot file size');
  check('parquetFileSize', 1 * MB, R2_SINGLE_OBJECT_LIMIT, 'Parquet file size');

  return { valid: errors.length === 0, errors };
}

// =============================================================================
// Size Parsing & Formatting
// =============================================================================

const SIZE_UNITS: Record<string, number> = {
  b: 1,
  kb: KB,
  mb: MB,
  gb: GB,
};

/**
 * Parse a human-readable size string to bytes.
 *
 * Supported formats: "256KB", "4MB", "2GB", "1048576" (bare bytes)
 */
export function parseStorageSize(input: string): number {
  if (!input || input.trim().length === 0) {
    throw new Error('Empty storage size string');
  }

  const trimmed = input.trim();
  const match = trimmed.match(/^(\d+(?:\.\d+)?)\s*(KB|MB|GB|B)?$/i);

  if (!match) {
    throw new Error(`Invalid storage size format: "${input}". Use format like "4MB", "256KB", "2GB", or bytes as number.`);
  }

  const value = parseFloat(match[1]);
  const unit = (match[2] || 'b').toLowerCase();

  if (value < 0) {
    throw new Error(`Storage size must be non-negative: "${input}"`);
  }

  const multiplier = SIZE_UNITS[unit];
  if (multiplier === undefined) {
    throw new Error(`Unknown size unit: "${unit}"`);
  }

  return Math.round(value * multiplier);
}

/**
 * Format bytes to human-readable size string.
 */
export function formatStorageSize(bytes: number): string {
  if (bytes >= GB && bytes % GB === 0) return `${bytes / GB}GB`;
  if (bytes >= MB && bytes % MB === 0) return `${bytes / MB}MB`;
  if (bytes >= MB) return `${(bytes / MB).toFixed(bytes % MB === 0 ? 0 : 1)}MB`;
  if (bytes >= KB && bytes % KB === 0) return `${bytes / KB}KB`;
  return `${bytes}B`;
}

// =============================================================================
// SQL Parsing
// =============================================================================

/** Valid storage configuration keys */
const VALID_STORAGE_CONFIG_KEYS = new Set<keyof StorageConfig>([
  'chunkSize',
  'maxPageSize',
  'rowGroupSize',
  'maxRowsPerRowGroup',
  'hotStorageMaxSize',
  'hotDataMaxAge',
  'maxHotFileSize',
  'parquetFileSize',
]);

/**
 * Parse WITH STORAGE clause from CREATE TABLE SQL.
 *
 * Returns null if no WITH STORAGE clause is present.
 * Returns Partial<StorageConfig> with parsed settings.
 *
 * Example:
 *   CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = '4MB', parquetFileSize = '256MB')
 *
 * Syntax:
 *   WITH STORAGE (key = value, key = value, ...)
 *
 * Values can be:
 *   - Quoted size strings: '4MB', "256KB", '2GB'
 *   - Unquoted size strings: 4MB, 256KB, 2GB
 *   - Bare numbers: 1048576 (interpreted as bytes)
 *
 * @param sql - The CREATE TABLE SQL statement
 * @returns Parsed storage config or null if no clause present
 * @throws Error if unknown settings or invalid values are encountered
 */
export function parseWithStorageClause(sql: string): TableStorageConfig | null {
  // Extract WITH STORAGE (...) clause (case insensitive)
  const withStorageMatch = sql.match(/\bWITH\s+STORAGE\s*\(\s*([^)]+)\s*\)/i);

  if (!withStorageMatch) {
    return null;
  }

  const settingsStr = withStorageMatch[1];
  const config: TableStorageConfig = {};

  // Parse key = value pairs
  // Matches: key = 'value' or key = "value" or key = value
  // Captures: (1) key, (2) optional quote, (3) value
  const settingRegex = /(\w+)\s*=\s*(['"]?)([^,'"]+)\2/g;
  let match: RegExpExecArray | null;

  while ((match = settingRegex.exec(settingsStr)) !== null) {
    const key = match[1].trim();
    const value = match[3].trim();

    // Validate setting name
    if (!VALID_STORAGE_CONFIG_KEYS.has(key as keyof StorageConfig)) {
      const validKeys = Array.from(VALID_STORAGE_CONFIG_KEYS).sort().join(', ');
      throw new Error(
        `Unknown storage setting: "${key}". Valid settings are: ${validKeys}`
      );
    }

    // Parse value - try as bare number first, then as storage size
    const parsedValue = /^\d+$/.test(value)
      ? parseInt(value, 10)
      : parseStorageSize(value);

    // Add to config (type assertion is safe due to validation above)
    config[key as keyof StorageConfig] = parsedValue;
  }

  return config;
}
