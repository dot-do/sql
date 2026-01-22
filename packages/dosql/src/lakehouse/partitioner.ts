/**
 * Lakehouse Partitioner for DoSQL
 *
 * Manages partition assignment and directory structure for lakehouse data.
 * Supports partitioning by time (year/month/day/hour) and by key (tenantId, region, etc.).
 */

import {
  type PartitionColumn,
  type PartitionKey,
  type Partition,
  type ChunkMetadata,
  buildPartitionPath,
  parsePartitionPath,
  LakehouseError,
  LakehouseErrorCode,
} from './types.js';

// =============================================================================
// Partition Value Extraction
// =============================================================================

/**
 * Extract partition value from a row based on partition column definition
 */
export function extractPartitionValue(
  row: Record<string, unknown>,
  column: PartitionColumn
): string {
  const rawValue = row[column.name];

  if (rawValue === null || rawValue === undefined) {
    return '__null__';
  }

  // Apply transform
  switch (column.transform) {
    case 'year':
      return extractYear(rawValue, column.type);
    case 'month':
      return extractMonth(rawValue, column.type);
    case 'day':
      return extractDay(rawValue, column.type);
    case 'hour':
      return extractHour(rawValue, column.type);
    case 'bucket':
      return applyBucket(rawValue, column.transformArg ?? 16);
    case 'truncate':
      return applyTruncate(rawValue, column.transformArg ?? 10);
    case 'identity':
    default:
      return formatPartitionValue(rawValue, column.type);
  }
}

/**
 * Extract year from a date/timestamp value
 */
function extractYear(value: unknown, type: string): string {
  const date = toDate(value, type);
  return date.getUTCFullYear().toString();
}

/**
 * Extract year-month from a date/timestamp value
 */
function extractMonth(value: unknown, type: string): string {
  const date = toDate(value, type);
  const year = date.getUTCFullYear();
  const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
  return `${year}-${month}`;
}

/**
 * Extract year-month-day from a date/timestamp value
 */
function extractDay(value: unknown, type: string): string {
  const date = toDate(value, type);
  const year = date.getUTCFullYear();
  const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
  const day = date.getUTCDate().toString().padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Extract year-month-day-hour from a date/timestamp value
 */
function extractHour(value: unknown, type: string): string {
  const date = toDate(value, type);
  const year = date.getUTCFullYear();
  const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
  const day = date.getUTCDate().toString().padStart(2, '0');
  const hour = date.getUTCHours().toString().padStart(2, '0');
  return `${year}-${month}-${day}-${hour}`;
}

/**
 * Convert value to Date object
 */
function toDate(value: unknown, type: string): Date {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    // Assume Unix timestamp in milliseconds
    return new Date(Number(value));
  }
  if (typeof value === 'string') {
    return new Date(value);
  }
  throw new LakehouseError(
    LakehouseErrorCode.CONFIG_ERROR,
    `Cannot convert value to date: ${typeof value}`
  );
}

/**
 * Apply bucket transform (hash mod N)
 */
function applyBucket(value: unknown, bucketCount: number): string {
  const hash = simpleHash(String(value));
  const bucket = Math.abs(hash) % bucketCount;
  return bucket.toString().padStart(4, '0');
}

/**
 * Apply truncate transform (for strings: first N chars, for numbers: floor to multiple)
 */
function applyTruncate(value: unknown, length: number): string {
  if (typeof value === 'string') {
    return value.substring(0, length);
  }
  if (typeof value === 'number') {
    return (Math.floor(value / length) * length).toString();
  }
  if (typeof value === 'bigint') {
    return ((value / BigInt(length)) * BigInt(length)).toString();
  }
  return String(value).substring(0, length);
}

/**
 * Format partition value for path
 */
function formatPartitionValue(value: unknown, type: string): string {
  if (typeof value === 'string') {
    // Sanitize for filesystem path
    return sanitizePathComponent(value);
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString().split('T')[0];
  }
  return sanitizePathComponent(String(value));
}

/**
 * Sanitize a string for use in a filesystem path
 */
function sanitizePathComponent(value: string): string {
  // Replace unsafe characters with underscore
  return value
    .replace(/[\/\\:*?"<>|]/g, '_')
    .replace(/\s+/g, '_')
    .substring(0, 128); // Limit length
}

/**
 * Simple hash function (FNV-1a)
 */
function simpleHash(str: string): number {
  let hash = 2166136261;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = (hash * 16777619) >>> 0;
  }
  return hash;
}

// =============================================================================
// Partitioner Class
// =============================================================================

/**
 * Options for the partitioner
 */
export interface PartitionerOptions {
  /** Partition column definitions */
  columns: PartitionColumn[];
  /** Base path prefix */
  basePath?: string;
}

/**
 * Partitioner manages partition assignment for rows
 */
export class Partitioner {
  private readonly columns: PartitionColumn[];
  private readonly basePath: string;

  constructor(options: PartitionerOptions) {
    this.columns = options.columns;
    this.basePath = options.basePath ?? '';
  }

  /**
   * Get partition keys for a row
   */
  getPartitionKeys(row: Record<string, unknown>): PartitionKey[] {
    return this.columns.map(column => ({
      column: column.name,
      value: extractPartitionValue(row, column),
    }));
  }

  /**
   * Get partition path for a row
   */
  getPartitionPath(row: Record<string, unknown>): string {
    const keys = this.getPartitionKeys(row);
    const partitionPath = buildPartitionPath(keys);
    return this.basePath ? `${this.basePath}/${partitionPath}` : partitionPath;
  }

  /**
   * Get full chunk path
   */
  getChunkPath(row: Record<string, unknown>, chunkId: string): string {
    const partitionPath = this.getPartitionPath(row);
    return `${partitionPath}/${chunkId}.columnar`;
  }

  /**
   * Group rows by partition
   */
  groupByPartition(
    rows: Record<string, unknown>[]
  ): Map<string, { keys: PartitionKey[]; rows: Record<string, unknown>[] }> {
    const groups = new Map<string, { keys: PartitionKey[]; rows: Record<string, unknown>[] }>();

    for (const row of rows) {
      const keys = this.getPartitionKeys(row);
      const path = buildPartitionPath(keys);

      let group = groups.get(path);
      if (!group) {
        group = { keys, rows: [] };
        groups.set(path, group);
      }
      group.rows.push(row);
    }

    return groups;
  }

  /**
   * Check if partition keys match a filter
   */
  matchesFilter(
    keys: PartitionKey[],
    filter: Partial<Record<string, string | string[]>>
  ): boolean {
    for (const key of keys) {
      const filterValue = filter[key.column];
      if (filterValue === undefined) continue;

      if (Array.isArray(filterValue)) {
        if (!filterValue.includes(key.value)) {
          return false;
        }
      } else {
        if (key.value !== filterValue) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Get partition columns
   */
  getColumns(): PartitionColumn[] {
    return this.columns;
  }
}

// =============================================================================
// Partition Pruning
// =============================================================================

/**
 * Partition filter for pruning
 */
export interface PartitionFilter {
  /** Column name */
  column: string;
  /** Filter operator */
  op: 'eq' | 'in' | 'range' | 'prefix';
  /** Filter value(s) */
  value: string | string[] | { min?: string; max?: string };
}

/**
 * Prune partitions based on filters
 */
export function prunePartitions(
  partitions: Map<string, Partition>,
  filters: PartitionFilter[]
): Map<string, Partition> {
  if (filters.length === 0) {
    return partitions;
  }

  const result = new Map<string, Partition>();

  for (const [path, partition] of partitions) {
    if (matchesPartitionFilters(partition.keys, filters)) {
      result.set(path, partition);
    }
  }

  return result;
}

/**
 * Check if partition keys match all filters
 */
function matchesPartitionFilters(
  keys: PartitionKey[],
  filters: PartitionFilter[]
): boolean {
  for (const filter of filters) {
    const key = keys.find(k => k.column === filter.column);
    if (!key) continue; // Column not in partition keys

    if (!matchesPartitionFilter(key.value, filter)) {
      return false;
    }
  }
  return true;
}

/**
 * Check if a partition value matches a filter
 */
function matchesPartitionFilter(
  value: string,
  filter: PartitionFilter
): boolean {
  switch (filter.op) {
    case 'eq':
      return value === filter.value;

    case 'in':
      return Array.isArray(filter.value) && filter.value.includes(value);

    case 'range': {
      const range = filter.value as { min?: string; max?: string };
      if (range.min !== undefined && value < range.min) return false;
      if (range.max !== undefined && value > range.max) return false;
      return true;
    }

    case 'prefix':
      return typeof filter.value === 'string' && value.startsWith(filter.value);

    default:
      return true;
  }
}

// =============================================================================
// Partition Directory Structure
// =============================================================================

/**
 * Generate directory listing for a table's partitions
 */
export function generatePartitionStructure(
  tableName: string,
  partitions: Map<string, Partition>,
  prefix: string = ''
): string[] {
  const paths: string[] = [];
  const fullPrefix = prefix ? `${prefix}/${tableName}` : tableName;

  for (const [partitionPath, partition] of partitions) {
    // Add partition directory
    const fullPath = `${fullPrefix}/${partitionPath}`;
    paths.push(fullPath);

    // Add chunk files
    for (const chunk of partition.chunks) {
      paths.push(chunk.path);
    }
  }

  return paths.sort();
}

/**
 * Parse a chunk path to extract table, partition, and chunk info
 */
export interface ParsedChunkPath {
  table: string;
  partitionKeys: PartitionKey[];
  partitionPath: string;
  chunkId: string;
  fileName: string;
}

export function parseChunkPath(path: string, prefix: string = ''): ParsedChunkPath | null {
  // Remove prefix if present
  let relativePath = path;
  if (prefix && path.startsWith(prefix)) {
    relativePath = path.substring(prefix.length);
    if (relativePath.startsWith('/')) {
      relativePath = relativePath.substring(1);
    }
  }

  // Split path: table/partition=value/.../chunk.columnar
  const parts = relativePath.split('/').filter(Boolean);
  if (parts.length < 2) return null;

  const table = parts[0];
  const fileName = parts[parts.length - 1];

  // Extract chunk ID from filename
  if (!fileName.endsWith('.columnar')) return null;
  const chunkId = fileName.replace('.columnar', '');

  // Parse partition keys from middle parts
  const partitionParts = parts.slice(1, -1);
  const partitionKeys = parsePartitionPath(partitionParts.join('/'));
  const partitionPath = partitionParts.join('/');

  return {
    table,
    partitionKeys,
    partitionPath,
    chunkId,
    fileName,
  };
}

// =============================================================================
// Time-based Partition Helpers
// =============================================================================

/**
 * Generate time partition values for a range
 */
export function generateTimePartitions(
  start: Date,
  end: Date,
  granularity: 'year' | 'month' | 'day' | 'hour'
): string[] {
  const partitions: string[] = [];
  const current = new Date(start);

  while (current <= end) {
    switch (granularity) {
      case 'year':
        partitions.push(current.getUTCFullYear().toString());
        current.setUTCFullYear(current.getUTCFullYear() + 1);
        break;

      case 'month': {
        const year = current.getUTCFullYear();
        const month = (current.getUTCMonth() + 1).toString().padStart(2, '0');
        partitions.push(`${year}-${month}`);
        current.setUTCMonth(current.getUTCMonth() + 1);
        break;
      }

      case 'day': {
        const year = current.getUTCFullYear();
        const month = (current.getUTCMonth() + 1).toString().padStart(2, '0');
        const day = current.getUTCDate().toString().padStart(2, '0');
        partitions.push(`${year}-${month}-${day}`);
        current.setUTCDate(current.getUTCDate() + 1);
        break;
      }

      case 'hour': {
        const year = current.getUTCFullYear();
        const month = (current.getUTCMonth() + 1).toString().padStart(2, '0');
        const day = current.getUTCDate().toString().padStart(2, '0');
        const hour = current.getUTCHours().toString().padStart(2, '0');
        partitions.push(`${year}-${month}-${day}-${hour}`);
        current.setUTCHours(current.getUTCHours() + 1);
        break;
      }
    }
  }

  return partitions;
}

/**
 * Infer time granularity from partition values
 */
export function inferTimeGranularity(
  partitionValue: string
): 'year' | 'month' | 'day' | 'hour' | 'unknown' {
  // Year: 2024
  if (/^\d{4}$/.test(partitionValue)) {
    return 'year';
  }
  // Month: 2024-01
  if (/^\d{4}-\d{2}$/.test(partitionValue)) {
    return 'month';
  }
  // Day: 2024-01-15
  if (/^\d{4}-\d{2}-\d{2}$/.test(partitionValue)) {
    return 'day';
  }
  // Hour: 2024-01-15-12
  if (/^\d{4}-\d{2}-\d{2}-\d{2}$/.test(partitionValue)) {
    return 'hour';
  }
  return 'unknown';
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a time-based partitioner
 */
export function createTimePartitioner(
  columnName: string,
  granularity: 'year' | 'month' | 'day' | 'hour',
  basePath?: string
): Partitioner {
  return new Partitioner({
    columns: [{
      name: columnName,
      type: 'timestamp',
      transform: granularity,
    }],
    basePath,
  });
}

/**
 * Create a composite partitioner (time + key)
 */
export function createCompositePartitioner(
  timeColumn: string,
  timeGranularity: 'year' | 'month' | 'day' | 'hour',
  keyColumn: string,
  basePath?: string
): Partitioner {
  return new Partitioner({
    columns: [
      {
        name: timeColumn,
        type: 'timestamp',
        transform: timeGranularity,
      },
      {
        name: keyColumn,
        type: 'string',
        transform: 'identity',
      },
    ],
    basePath,
  });
}

/**
 * Create a bucket partitioner for high-cardinality keys
 */
export function createBucketPartitioner(
  columnName: string,
  bucketCount: number,
  basePath?: string
): Partitioner {
  return new Partitioner({
    columns: [{
      name: columnName,
      type: 'string',
      transform: 'bucket',
      transformArg: bucketCount,
    }],
    basePath,
  });
}
