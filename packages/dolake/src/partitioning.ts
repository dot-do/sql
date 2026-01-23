/**
 * DoLake Partitioning Module
 *
 * Provides table partitioning capabilities including:
 * - Time-based partitioning (year, month, day, hour)
 * - Hash-based partitioning (bucket)
 * - Composite partitioning (date + hash)
 * - Partition pruning for queries
 * - Partition metadata management
 */

import {
  type IcebergPartitionSpec,
  type IcebergPartitionField,
  type IcebergSchema,
  type IcebergTableMetadata,
  type DataFile,
  type CDCEvent,
  generateUUID,
} from './types.js';

// =============================================================================
// Partition Transform Types
// =============================================================================

/**
 * Supported partition transforms
 */
export type PartitionTransform =
  | 'identity'
  | 'year'
  | 'month'
  | 'day'
  | 'hour'
  | `bucket[${number}]`
  | 'truncate'
  | 'void';

/**
 * Partition value - result of applying transform
 */
export type PartitionValue = string | number | null;

/**
 * Partition key - combination of field name and value
 */
export interface PartitionKey {
  field: string;
  value: PartitionValue;
  transform: PartitionTransform;
}

/**
 * Full partition identifier
 */
export interface PartitionIdentifier {
  keys: PartitionKey[];
  path: string;
}

/**
 * Partition statistics
 */
export interface PartitionStats {
  partition: string;
  recordCount: bigint;
  fileCount: number;
  sizeBytes: bigint;
  lastModified: number;
  minTimestamp?: number;
  maxTimestamp?: number;
}

/**
 * Partition metadata for scalability
 */
export interface PartitionMetadata {
  partition: string;
  files: DataFile[];
  stats: PartitionStats;
  compactionPending: boolean;
  lastCompactionTime?: number;
  createdAt: number;
}

// =============================================================================
// Partition Transform Functions
// =============================================================================

/**
 * Apply year transform to timestamp
 */
export function yearTransform(timestamp: number): number {
  const date = new Date(timestamp);
  return date.getUTCFullYear();
}

/**
 * Apply month transform to timestamp
 */
export function monthTransform(timestamp: number): number {
  const date = new Date(timestamp);
  return date.getUTCFullYear() * 12 + date.getUTCMonth();
}

/**
 * Apply day transform to timestamp
 */
export function dayTransform(timestamp: number): number {
  // Days since Unix epoch
  return Math.floor(timestamp / (24 * 60 * 60 * 1000));
}

/**
 * Apply hour transform to timestamp
 */
export function hourTransform(timestamp: number): number {
  // Hours since Unix epoch
  return Math.floor(timestamp / (60 * 60 * 1000));
}

/**
 * Apply bucket transform (hash partitioning)
 */
export function bucketTransform(value: unknown, numBuckets: number): number {
  const hash = hashValue(value);
  // Ensure non-negative modulo
  return ((hash % numBuckets) + numBuckets) % numBuckets;
}

/**
 * Hash function for bucket partitioning
 */
export function hashValue(value: unknown): number {
  if (value === null || value === undefined) {
    return 0;
  }

  const str = String(value);
  let hash = 0;

  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }

  return Math.abs(hash);
}

/**
 * Apply truncate transform
 */
export function truncateTransform(value: string, width: number): string {
  return value.substring(0, width);
}

/**
 * Format day value as date string
 */
export function dayToDateString(daysSinceEpoch: number): string {
  const date = new Date(daysSinceEpoch * 24 * 60 * 60 * 1000);
  return date.toISOString().slice(0, 10);
}

/**
 * Format hour value as datetime string
 */
export function hourToDatetimeString(hoursSinceEpoch: number): string {
  const date = new Date(hoursSinceEpoch * 60 * 60 * 1000);
  return date.toISOString().slice(0, 13) + ':00';
}

// =============================================================================
// Partition Spec Creation
// =============================================================================

/**
 * Create a partition spec with day transform
 */
export function createDayPartitionSpec(
  sourceFieldId: number,
  fieldName: string = 'day',
  specId: number = 0
): IcebergPartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': sourceFieldId,
        'field-id': 1000,
        name: fieldName,
        transform: 'day',
      },
    ],
  };
}

/**
 * Create a partition spec with hour transform
 */
export function createHourPartitionSpec(
  sourceFieldId: number,
  fieldName: string = 'hour',
  specId: number = 0
): IcebergPartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': sourceFieldId,
        'field-id': 1000,
        name: fieldName,
        transform: 'hour',
      },
    ],
  };
}

/**
 * Create a partition spec with bucket transform
 */
export function createBucketPartitionSpec(
  sourceFieldId: number,
  numBuckets: number,
  fieldName: string,
  specId: number = 0
): IcebergPartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': sourceFieldId,
        'field-id': 1000,
        name: fieldName,
        transform: `bucket[${numBuckets}]`,
      },
    ],
  };
}

/**
 * Create a composite partition spec (date + hash)
 */
export function createCompositePartitionSpec(
  timeSourceId: number,
  timeTransform: 'year' | 'month' | 'day' | 'hour',
  hashSourceId: number,
  numBuckets: number,
  specId: number = 0
): IcebergPartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': timeSourceId,
        'field-id': 1000,
        name: timeTransform,
        transform: timeTransform,
      },
      {
        'source-id': hashSourceId,
        'field-id': 1001,
        name: `bucket_${numBuckets}`,
        transform: `bucket[${numBuckets}]`,
      },
    ],
  };
}

// =============================================================================
// Partition Value Computation
// =============================================================================

/**
 * Compute partition value from event data
 */
export function computePartitionValue(
  value: unknown,
  transform: PartitionTransform
): PartitionValue {
  if (value === null || value === undefined) {
    return null;
  }

  if (transform === 'identity') {
    return String(value);
  }

  if (transform === 'void') {
    return null;
  }

  // Time-based transforms
  if (transform === 'year' || transform === 'month' || transform === 'day' || transform === 'hour') {
    const timestamp = typeof value === 'number' ? value : new Date(String(value)).getTime();

    switch (transform) {
      case 'year':
        return yearTransform(timestamp);
      case 'month':
        return monthTransform(timestamp);
      case 'day':
        return dayTransform(timestamp);
      case 'hour':
        return hourTransform(timestamp);
    }
  }

  // Bucket transform
  const bucketMatch = transform.match(/^bucket\[(\d+)\]$/);
  if (bucketMatch && bucketMatch[1]) {
    const numBuckets = parseInt(bucketMatch[1], 10);
    return bucketTransform(value, numBuckets);
  }

  return String(value);
}

/**
 * Compute full partition identifier from event
 */
export function computePartitionIdentifier(
  event: CDCEvent,
  spec: IcebergPartitionSpec,
  schema: IcebergSchema
): PartitionIdentifier {
  const keys: PartitionKey[] = [];

  for (const field of spec.fields) {
    // Find source field in schema
    const sourceField = schema.fields.find((f) => f.id === field['source-id']);
    if (!sourceField) {
      continue;
    }

    // Get value from event data
    const data = event.after ?? event.before ?? {};
    const value = (data as Record<string, unknown>)[sourceField.name];

    // Compute partition value
    const partitionValue = computePartitionValue(value, field.transform as PartitionTransform);

    keys.push({
      field: field.name,
      value: partitionValue,
      transform: field.transform as PartitionTransform,
    });
  }

  // Build path
  const path = keys
    .map((k) => `${k.field}=${k.value ?? '__HIVE_DEFAULT_PARTITION__'}`)
    .join('/');

  return { keys, path };
}

/**
 * Get partition key as string for indexing
 */
export function partitionKeyToString(identifier: PartitionIdentifier): string {
  return identifier.path;
}

// =============================================================================
// Partition Pruning
// =============================================================================

/**
 * SQL predicate for partition pruning
 */
export interface PartitionPredicate {
  field: string;
  operator: '=' | '!=' | '<' | '<=' | '>' | '>=' | 'IN' | 'BETWEEN';
  value: PartitionValue | PartitionValue[];
  endValue?: PartitionValue; // For BETWEEN
}

/**
 * Query plan with partition information
 */
export interface QueryPlan {
  partitionsIncluded: string[];
  partitionsPruned: string[];
  pruningRatio: number;
  filesScanned: number;
  filesSkippedByStats: number;
  totalFilesConsidered: number;
}

/**
 * Check if a partition matches a predicate
 */
export function partitionMatchesPredicate(
  partition: string,
  predicate: PartitionPredicate
): boolean {
  // Parse partition path to get field value
  const parts = partition.split('/');
  for (const part of parts) {
    const [field, valueStr] = part.split('=');
    if (!field || field !== predicate.field) {
      continue;
    }

    const value = valueStr === '__HIVE_DEFAULT_PARTITION__' || valueStr === undefined ? null : valueStr;

    switch (predicate.operator) {
      case '=':
        return value === String(predicate.value);

      case '!=':
        return value !== String(predicate.value);

      case '<':
        return value !== null && value < String(predicate.value);

      case '<=':
        return value !== null && value <= String(predicate.value);

      case '>':
        return value !== null && value > String(predicate.value);

      case '>=':
        return value !== null && value >= String(predicate.value);

      case 'IN':
        if (!Array.isArray(predicate.value)) {
          return value === String(predicate.value);
        }
        return predicate.value.some((v) => value === String(v));

      case 'BETWEEN':
        if (value === null) return false;
        return value >= String(predicate.value) && value <= String(predicate.endValue);
    }
  }

  return true; // No matching field, include partition
}

/**
 * Prune partitions based on predicates
 */
export function prunePartitions(
  allPartitions: string[],
  predicates: PartitionPredicate[]
): { included: string[]; pruned: string[] } {
  const included: string[] = [];
  const pruned: string[] = [];

  for (const partition of allPartitions) {
    const matches = predicates.every((pred) =>
      partitionMatchesPredicate(partition, pred)
    );

    if (matches) {
      included.push(partition);
    } else {
      pruned.push(partition);
    }
  }

  return { included, pruned };
}

// =============================================================================
// WHERE Clause Parsing Helpers
// =============================================================================

/**
 * Parse equality predicates: field = 'value' or field = value
 */
function parseEqualityPredicates(sql: string): PartitionPredicate[] {
  const predicates: PartitionPredicate[] = [];
  const matches = sql.matchAll(/(\w+)\s*=\s*'?([^'\s]+)'?/g);
  for (const match of matches) {
    if (match[1] && match[2]) {
      predicates.push({
        field: match[1],
        operator: '=',
        value: match[2],
      });
    }
  }
  return predicates;
}

/**
 * Parse IN predicates: field IN ('a', 'b', 'c')
 */
function parseInPredicates(sql: string): PartitionPredicate[] {
  const predicates: PartitionPredicate[] = [];
  const matches = sql.matchAll(/(\w+)\s+IN\s*\(([^)]+)\)/gi);
  for (const match of matches) {
    if (match[1] && match[2]) {
      const values = match[2].split(',').map((v) => v.trim().replace(/'/g, ''));
      predicates.push({
        field: match[1],
        operator: 'IN',
        value: values,
      });
    }
  }
  return predicates;
}

/**
 * Parse BETWEEN predicates: field BETWEEN 'a' AND 'b'
 */
function parseBetweenPredicates(sql: string): PartitionPredicate[] {
  const predicates: PartitionPredicate[] = [];
  const matches = sql.matchAll(/(\w+)\s+BETWEEN\s+'?([^'\s]+)'?\s+AND\s+'?([^'\s]+)'?/gi);
  for (const match of matches) {
    if (match[1] && match[2] && match[3]) {
      predicates.push({
        field: match[1],
        operator: 'BETWEEN',
        value: match[2],
        endValue: match[3],
      });
    }
  }
  return predicates;
}

/**
 * Parse greater-than-or-equal predicates: field >= value
 */
function parseGreaterOrEqualPredicates(sql: string): PartitionPredicate[] {
  const predicates: PartitionPredicate[] = [];
  const matches = sql.matchAll(/(\w+)\s*>=\s*'?([^'\s]+)'?/g);
  for (const match of matches) {
    if (match[1] && match[2]) {
      predicates.push({
        field: match[1],
        operator: '>=',
        value: match[2],
      });
    }
  }
  return predicates;
}

/**
 * Parse less-than-or-equal predicates: field <= value
 */
function parseLessOrEqualPredicates(sql: string): PartitionPredicate[] {
  const predicates: PartitionPredicate[] = [];
  const matches = sql.matchAll(/(\w+)\s*<=\s*'?([^'\s]+)'?/g);
  for (const match of matches) {
    if (match[1] && match[2]) {
      predicates.push({
        field: match[1],
        operator: '<=',
        value: match[2],
      });
    }
  }
  return predicates;
}

/**
 * Parse greater-than predicates: field > value (excluding >=)
 */
function parseGreaterThanPredicates(sql: string): PartitionPredicate[] {
  const predicates: PartitionPredicate[] = [];
  const matches = sql.matchAll(/(\w+)\s*>\s*'?([^'\s>=]+)'?/g);
  for (const match of matches) {
    // Skip if it's part of >= which is handled separately
    const fullMatch = match[0];
    if (!fullMatch.includes('>=') && match[1] && match[2]) {
      predicates.push({
        field: match[1],
        operator: '>',
        value: match[2],
      });
    }
  }
  return predicates;
}

/**
 * Parse less-than predicates: field < value (excluding <=)
 */
function parseLessThanPredicates(sql: string): PartitionPredicate[] {
  const predicates: PartitionPredicate[] = [];
  const matches = sql.matchAll(/(\w+)\s*<\s*'?([^'\s<=]+)'?/g);
  for (const match of matches) {
    // Skip if it's part of <= which is handled separately
    const fullMatch = match[0];
    if (!fullMatch.includes('<=') && match[1] && match[2]) {
      predicates.push({
        field: match[1],
        operator: '<',
        value: match[2],
      });
    }
  }
  return predicates;
}

/**
 * Parse simple SQL WHERE clause to extract partition predicates
 * This is a simplified parser for demonstration
 */
export function parseWhereClause(sql: string): PartitionPredicate[] {
  return [
    ...parseEqualityPredicates(sql),
    ...parseInPredicates(sql),
    ...parseBetweenPredicates(sql),
    ...parseGreaterOrEqualPredicates(sql),
    ...parseLessOrEqualPredicates(sql),
    ...parseGreaterThanPredicates(sql),
    ...parseLessThanPredicates(sql),
  ];
}

// =============================================================================
// Partition Manager
// =============================================================================

/**
 * Partition manager configuration
 */
export interface PartitionManagerConfig {
  maxPartitionsPerTable: number;
  partitionCacheTTLMs: number;
  enableMetadataCaching: boolean;
  paginationPageSize: number;
}

/**
 * Default partition manager configuration
 */
export const DEFAULT_PARTITION_MANAGER_CONFIG: PartitionManagerConfig = {
  maxPartitionsPerTable: 100000,
  partitionCacheTTLMs: 60000, // 1 minute
  enableMetadataCaching: true,
  paginationPageSize: 100,
};

/**
 * Partition list response with pagination
 */
export interface PartitionListResponse {
  partitions: string[];
  nextPageToken: string | null;
  totalCount: number;
}

/**
 * Partition manager for scalable partition handling
 */
export class PartitionManager {
  private readonly config: PartitionManagerConfig;
  private readonly partitionCache: Map<string, { data: PartitionMetadata[]; timestamp: number }>;
  private readonly partitionIndex: Map<string, Set<string>>; // table -> partitions

  constructor(config: PartitionManagerConfig = DEFAULT_PARTITION_MANAGER_CONFIG) {
    this.config = config;
    this.partitionCache = new Map();
    this.partitionIndex = new Map();
  }

  /**
   * Register a new partition
   */
  registerPartition(tableName: string, partition: string): void {
    let partitions = this.partitionIndex.get(tableName);
    if (!partitions) {
      partitions = new Set();
      this.partitionIndex.set(tableName, partitions);
    }
    partitions.add(partition);
  }

  /**
   * Get all partitions for a table
   */
  getPartitions(tableName: string): string[] {
    const partitions = this.partitionIndex.get(tableName);
    return partitions ? Array.from(partitions) : [];
  }

  /**
   * List partitions with pagination
   */
  listPartitions(
    tableName: string,
    pageSize: number = this.config.paginationPageSize,
    pageToken?: string
  ): PartitionListResponse {
    const allPartitions = this.getPartitions(tableName).sort();
    const totalCount = allPartitions.length;

    let startIndex = 0;
    if (pageToken) {
      startIndex = parseInt(pageToken, 10);
    }

    const endIndex = Math.min(startIndex + pageSize, totalCount);
    const partitions = allPartitions.slice(startIndex, endIndex);

    const nextPageToken = endIndex < totalCount ? String(endIndex) : null;

    return {
      partitions,
      nextPageToken,
      totalCount,
    };
  }

  /**
   * Get partition count
   */
  getPartitionCount(tableName: string): number {
    return this.partitionIndex.get(tableName)?.size ?? 0;
  }

  /**
   * Check if partition exists
   */
  hasPartition(tableName: string, partition: string): boolean {
    return this.partitionIndex.get(tableName)?.has(partition) ?? false;
  }

  /**
   * Remove partition
   */
  removePartition(tableName: string, partition: string): void {
    this.partitionIndex.get(tableName)?.delete(partition);
  }

  /**
   * Clear all partitions for a table
   */
  clearPartitions(tableName: string): void {
    this.partitionIndex.delete(tableName);
    this.partitionCache.delete(tableName);
  }

  /**
   * Get cached partition metadata
   */
  getCachedMetadata(tableName: string): PartitionMetadata[] | null {
    if (!this.config.enableMetadataCaching) {
      return null;
    }

    const cached = this.partitionCache.get(tableName);
    if (!cached) {
      return null;
    }

    if (Date.now() - cached.timestamp > this.config.partitionCacheTTLMs) {
      this.partitionCache.delete(tableName);
      return null;
    }

    return cached.data;
  }

  /**
   * Set cached partition metadata
   */
  setCachedMetadata(tableName: string, metadata: PartitionMetadata[]): void {
    if (!this.config.enableMetadataCaching) {
      return;
    }

    this.partitionCache.set(tableName, {
      data: metadata,
      timestamp: Date.now(),
    });
  }

  /**
   * Invalidate cache for table
   */
  invalidateCache(tableName: string): void {
    this.partitionCache.delete(tableName);
  }

  /**
   * Create test partitions (for scalability testing)
   */
  createTestPartitions(tableName: string, numPartitions: number): void {
    const partitions = this.partitionIndex.get(tableName) ?? new Set<string>();

    for (let i = 0; i < numPartitions; i++) {
      const day = new Date(2024, 0, 1);
      day.setDate(day.getDate() + (i % 365));
      const dateStr = day.toISOString().slice(0, 10);
      partitions.add(`day=${dateStr}`);
    }

    this.partitionIndex.set(tableName, partitions);
  }

  /**
   * Create bucket partitions (for hash-based partitioning tests)
   */
  createBucketPartitions(tableName: string, numBuckets: number): void {
    const partitions = this.partitionIndex.get(tableName) ?? new Set<string>();

    for (let i = 0; i < numBuckets; i++) {
      partitions.add(`customer_bucket=${i}`);
    }

    this.partitionIndex.set(tableName, partitions);
  }
}

// =============================================================================
// Partition Statistics
// =============================================================================

/**
 * Calculate partition distribution statistics
 */
export function calculatePartitionStats(
  partitionSizes: Map<string, bigint>
): { skewRatio: number; hotPartitions: string[] } {
  if (partitionSizes.size === 0) {
    return { skewRatio: 1.0, hotPartitions: [] };
  }

  const sizes = Array.from(partitionSizes.values()).map(Number);
  const avg = sizes.reduce((a, b) => a + b, 0) / sizes.length;
  const max = Math.max(...sizes);
  const min = Math.min(...sizes);

  // Skew ratio: max/avg (1.0 = perfectly even)
  const skewRatio = avg > 0 ? max / avg : 1.0;

  // Hot partitions: those significantly larger than average
  const threshold = avg * 2;
  const hotPartitions: string[] = [];

  for (const [partition, size] of partitionSizes) {
    if (Number(size) > threshold) {
      hotPartitions.push(partition);
    }
  }

  return { skewRatio, hotPartitions };
}

/**
 * Calculate bucket distribution for hash partitioning
 */
export function calculateBucketDistribution(
  bucketCounts: number[]
): { skewRatio: number; isEven: boolean } {
  if (bucketCounts.length === 0) {
    return { skewRatio: 1.0, isEven: true };
  }

  const avg = bucketCounts.reduce((a, b) => a + b, 0) / bucketCounts.length;
  const max = Math.max(...bucketCounts);

  const skewRatio = avg > 0 ? max / avg : 1.0;
  const isEven = skewRatio < 1.5;

  return { skewRatio, isEven };
}

