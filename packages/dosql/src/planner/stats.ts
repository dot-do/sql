/**
 * DoSQL Cost-Based Query Planner - Table Statistics
 *
 * Provides statistics collection and management for:
 * - Table row counts and sizes
 * - Column cardinality and distribution
 * - Index statistics
 * - Histogram-based estimation
 */

import type { SqlValue } from '../engine/types.js';

// =============================================================================
// TABLE STATISTICS
// =============================================================================

/**
 * Statistics for a single table
 */
export interface TableStatistics {
  /** Table name */
  tableName: string;

  /** Total number of rows */
  rowCount: number;

  /** Number of pages (blocks) in the table */
  pageCount: number;

  /** Average row size in bytes */
  avgRowSize: number;

  /** Total table size in bytes */
  totalBytes: number;

  /** Timestamp when statistics were last updated */
  lastAnalyzed: Date;

  /** Column-level statistics */
  columns: Map<string, ColumnStatistics>;
}

/**
 * Statistics for a single column
 */
export interface ColumnStatistics {
  /** Column name */
  columnName: string;

  /** Number of distinct values */
  distinctCount: number;

  /** Fraction of null values (0-1) */
  nullFraction: number;

  /** Average width in bytes */
  avgWidth: number;

  /** Minimum value (if available) */
  minValue?: SqlValue;

  /** Maximum value (if available) */
  maxValue?: SqlValue;

  /** Most common values and their frequencies */
  mostCommonValues?: MostCommonValue[];

  /** Histogram for value distribution */
  histogram?: HistogramBucket[];

  /** Correlation with physical row order (-1 to 1) */
  correlation?: number;
}

/**
 * A most common value entry
 */
export interface MostCommonValue {
  /** The value */
  value: SqlValue;

  /** Frequency (fraction of rows with this value) */
  frequency: number;
}

/**
 * A histogram bucket for equi-depth histograms
 */
export interface HistogramBucket {
  /** Lower bound (inclusive) */
  low: SqlValue;

  /** Upper bound (exclusive for numeric, inclusive for last bucket) */
  high: SqlValue;

  /** Number of rows in this bucket */
  count: number;

  /** Number of distinct values in this bucket */
  distinctCount: number;
}

// =============================================================================
// INDEX STATISTICS
// =============================================================================

/**
 * Statistics for an index
 */
export interface IndexStatistics {
  /** Index name */
  indexName: string;

  /** Table name */
  tableName: string;

  /** Number of index entries */
  entryCount: number;

  /** Number of distinct key values */
  distinctKeys: number;

  /** Height of the B-tree */
  treeHeight: number;

  /** Number of leaf pages */
  leafPages: number;

  /** Total pages in the index */
  totalPages: number;

  /** Average key size in bytes */
  avgKeySize: number;

  /** Clustering factor (correlation between index and heap order) */
  clusteringFactor: number;

  /** Timestamp when statistics were last updated */
  lastAnalyzed: Date;

  /** Per-column statistics for composite indexes */
  columnStats?: Map<string, IndexColumnStats>;
}

/**
 * Statistics for a column within a composite index
 */
export interface IndexColumnStats {
  /** Column name */
  columnName: string;

  /** Position in the index (0-based) */
  position: number;

  /** Number of distinct values for this prefix */
  distinctCount: number;

  /** Average number of rows per distinct value */
  avgRowsPerValue: number;
}

// =============================================================================
// STATISTICS STORE
// =============================================================================

/**
 * In-memory statistics store
 */
export class StatisticsStore {
  private tableStats = new Map<string, TableStatistics>();
  private indexStats = new Map<string, IndexStatistics>();

  /**
   * Get statistics for a table
   */
  getTableStats(tableName: string): TableStatistics | undefined {
    return this.tableStats.get(tableName);
  }

  /**
   * Set statistics for a table
   */
  setTableStats(stats: TableStatistics): void {
    this.tableStats.set(stats.tableName, stats);
  }

  /**
   * Get statistics for an index
   */
  getIndexStats(indexName: string): IndexStatistics | undefined {
    return this.indexStats.get(indexName);
  }

  /**
   * Set statistics for an index
   */
  setIndexStats(stats: IndexStatistics): void {
    this.indexStats.set(stats.indexName, stats);
  }

  /**
   * Get all table statistics
   */
  getAllTableStats(): Map<string, TableStatistics> {
    return new Map(this.tableStats);
  }

  /**
   * Get all index statistics
   */
  getAllIndexStats(): Map<string, IndexStatistics> {
    return new Map(this.indexStats);
  }

  /**
   * Get indexes for a table
   */
  getIndexesForTable(tableName: string): IndexStatistics[] {
    return Array.from(this.indexStats.values()).filter(
      stats => stats.tableName === tableName
    );
  }

  /**
   * Clear all statistics
   */
  clear(): void {
    this.tableStats.clear();
    this.indexStats.clear();
  }

  /**
   * Check if statistics exist for a table
   */
  hasTableStats(tableName: string): boolean {
    return this.tableStats.has(tableName);
  }

  /**
   * Check if statistics exist for an index
   */
  hasIndexStats(indexName: string): boolean {
    return this.indexStats.has(indexName);
  }

  /**
   * Get row count for a table (with default fallback)
   */
  getRowCount(tableName: string, defaultCount = 1000): number {
    return this.tableStats.get(tableName)?.rowCount ?? defaultCount;
  }

  /**
   * Get distinct count for a column (with default fallback)
   */
  getDistinctCount(tableName: string, columnName: string, defaultCount?: number): number {
    const tableStats = this.tableStats.get(tableName);
    if (!tableStats) {
      return defaultCount ?? Math.round(this.getRowCount(tableName) * 0.1);
    }

    const colStats = tableStats.columns.get(columnName);
    if (!colStats) {
      return defaultCount ?? Math.round(tableStats.rowCount * 0.1);
    }

    return colStats.distinctCount;
  }

  /**
   * Get null fraction for a column
   */
  getNullFraction(tableName: string, columnName: string): number {
    const tableStats = this.tableStats.get(tableName);
    if (!tableStats) return 0;

    const colStats = tableStats.columns.get(columnName);
    return colStats?.nullFraction ?? 0;
  }

  /**
   * Get histogram for a column
   */
  getHistogram(tableName: string, columnName: string): HistogramBucket[] | undefined {
    const tableStats = this.tableStats.get(tableName);
    if (!tableStats) return undefined;

    const colStats = tableStats.columns.get(columnName);
    return colStats?.histogram;
  }

  /**
   * Get most common values for a column
   */
  getMostCommonValues(tableName: string, columnName: string): MostCommonValue[] | undefined {
    const tableStats = this.tableStats.get(tableName);
    if (!tableStats) return undefined;

    const colStats = tableStats.columns.get(columnName);
    return colStats?.mostCommonValues;
  }
}

// =============================================================================
// SELECTIVITY ESTIMATION
// =============================================================================

/**
 * Estimate selectivity for an equality predicate
 */
export function estimateEqualitySelectivity(
  stats: StatisticsStore,
  tableName: string,
  columnName: string,
  value: SqlValue,
  defaultSelectivity = 0.01
): number {
  const tableStats = stats.getTableStats(tableName);
  if (!tableStats) return defaultSelectivity;

  const colStats = tableStats.columns.get(columnName);
  if (!colStats) return defaultSelectivity;

  // Check most common values first
  if (colStats.mostCommonValues) {
    const mcv = colStats.mostCommonValues.find(v => v.value === value);
    if (mcv) {
      return mcv.frequency;
    }
  }

  // Use distinct count for uniform distribution assumption
  if (colStats.distinctCount > 0) {
    const uniformSelectivity = 1 / colStats.distinctCount;
    return uniformSelectivity * (1 - colStats.nullFraction);
  }

  return defaultSelectivity;
}

/**
 * Estimate selectivity for a range predicate
 */
export function estimateRangeSelectivity(
  stats: StatisticsStore,
  tableName: string,
  columnName: string,
  operator: 'lt' | 'le' | 'gt' | 'ge',
  value: SqlValue,
  defaultSelectivity = 0.33
): number {
  const tableStats = stats.getTableStats(tableName);
  if (!tableStats) return defaultSelectivity;

  const colStats = tableStats.columns.get(columnName);
  if (!colStats) return defaultSelectivity;

  // Use histogram if available
  if (colStats.histogram && colStats.histogram.length > 0) {
    return estimateRangeFromHistogram(colStats.histogram, operator, value);
  }

  // Use min/max for simple estimation
  if (colStats.minValue !== undefined && colStats.maxValue !== undefined) {
    return estimateRangeFromMinMax(
      colStats.minValue,
      colStats.maxValue,
      operator,
      value
    );
  }

  return defaultSelectivity;
}

/**
 * Estimate selectivity for BETWEEN predicate
 */
export function estimateBetweenSelectivity(
  stats: StatisticsStore,
  tableName: string,
  columnName: string,
  low: SqlValue,
  high: SqlValue,
  defaultSelectivity = 0.25
): number {
  const tableStats = stats.getTableStats(tableName);
  if (!tableStats) return defaultSelectivity;

  const colStats = tableStats.columns.get(columnName);
  if (!colStats) return defaultSelectivity;

  // Use histogram if available
  if (colStats.histogram && colStats.histogram.length > 0) {
    return estimateBetweenFromHistogram(colStats.histogram, low, high);
  }

  // Use min/max for simple estimation
  if (colStats.minValue !== undefined && colStats.maxValue !== undefined) {
    return estimateBetweenFromMinMax(colStats.minValue, colStats.maxValue, low, high);
  }

  return defaultSelectivity;
}

/**
 * Estimate selectivity for IN predicate
 */
export function estimateInSelectivity(
  stats: StatisticsStore,
  tableName: string,
  columnName: string,
  values: SqlValue[],
  defaultSelectivity = 0.1
): number {
  if (values.length === 0) return 0;

  // Sum individual equality selectivities (with overlap adjustment)
  let totalSelectivity = 0;
  for (const value of values) {
    totalSelectivity += estimateEqualitySelectivity(
      stats,
      tableName,
      columnName,
      value,
      defaultSelectivity / values.length
    );
  }

  // Cap at 1.0
  return Math.min(1, totalSelectivity);
}

/**
 * Estimate selectivity for LIKE predicate
 */
export function estimateLikeSelectivity(
  pattern: string,
  defaultSelectivity = 0.1
): number {
  // Prefix-only patterns (e.g., 'abc%') are more selective
  if (!pattern.startsWith('%') && pattern.endsWith('%')) {
    const prefixLength = pattern.length - 1;
    // Rough estimate: each prefix character reduces selectivity
    return Math.pow(0.1, Math.min(prefixLength, 3));
  }

  // Patterns with leading wildcard are less selective
  if (pattern.startsWith('%')) {
    return defaultSelectivity;
  }

  // Contains patterns (e.g., '%abc%')
  if (pattern.includes('%')) {
    return defaultSelectivity * 0.5;
  }

  // Exact match (no wildcards)
  return 0.01;
}

/**
 * Estimate selectivity for IS NULL predicate
 */
export function estimateIsNullSelectivity(
  stats: StatisticsStore,
  tableName: string,
  columnName: string,
  isNot: boolean
): number {
  const nullFraction = stats.getNullFraction(tableName, columnName);
  return isNot ? 1 - nullFraction : nullFraction;
}

// =============================================================================
// HISTOGRAM UTILITIES
// =============================================================================

/**
 * Estimate range selectivity using histogram
 */
function estimateRangeFromHistogram(
  histogram: HistogramBucket[],
  operator: 'lt' | 'le' | 'gt' | 'ge',
  value: SqlValue
): number {
  const totalRows = histogram.reduce((sum, b) => sum + b.count, 0);
  if (totalRows === 0) return 0.33;

  let matchingRows = 0;

  for (const bucket of histogram) {
    const bucketFraction = estimateBucketOverlap(bucket, operator, value);
    matchingRows += bucket.count * bucketFraction;
  }

  return matchingRows / totalRows;
}

/**
 * Estimate BETWEEN selectivity using histogram
 */
function estimateBetweenFromHistogram(
  histogram: HistogramBucket[],
  low: SqlValue,
  high: SqlValue
): number {
  const totalRows = histogram.reduce((sum, b) => sum + b.count, 0);
  if (totalRows === 0) return 0.25;

  let matchingRows = 0;

  for (const bucket of histogram) {
    const bucketFraction = estimateBucketBetweenOverlap(bucket, low, high);
    matchingRows += bucket.count * bucketFraction;
  }

  return matchingRows / totalRows;
}

/**
 * Estimate how much of a bucket overlaps with a range condition
 */
function estimateBucketOverlap(
  bucket: HistogramBucket,
  operator: 'lt' | 'le' | 'gt' | 'ge',
  value: SqlValue
): number {
  const bucketLow = bucket.low;
  const bucketHigh = bucket.high;

  // Compare values (simplified for numbers)
  const compareVal = (a: SqlValue, b: SqlValue): number => {
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b);
    if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime();
    return 0;
  };

  const valInBucket = compareVal(value, bucketLow) >= 0 && compareVal(value, bucketHigh) < 0;

  switch (operator) {
    case 'lt':
      if (compareVal(value, bucketLow) <= 0) return 0;
      if (compareVal(value, bucketHigh) >= 0) return 1;
      // Linear interpolation within bucket
      return linearInterpolate(bucketLow, bucketHigh, value);

    case 'le':
      if (compareVal(value, bucketLow) < 0) return 0;
      if (compareVal(value, bucketHigh) >= 0) return 1;
      return linearInterpolate(bucketLow, bucketHigh, value);

    case 'gt':
      if (compareVal(value, bucketHigh) >= 0) return 0;
      if (compareVal(value, bucketLow) <= 0) return 1;
      return 1 - linearInterpolate(bucketLow, bucketHigh, value);

    case 'ge':
      if (compareVal(value, bucketHigh) > 0) return 0;
      if (compareVal(value, bucketLow) <= 0) return 1;
      return 1 - linearInterpolate(bucketLow, bucketHigh, value);

    default:
      return 0.5;
  }
}

/**
 * Estimate how much of a bucket overlaps with a BETWEEN range
 */
function estimateBucketBetweenOverlap(
  bucket: HistogramBucket,
  low: SqlValue,
  high: SqlValue
): number {
  const lowOverlap = estimateBucketOverlap(bucket, 'ge', low);
  const highOverlap = estimateBucketOverlap(bucket, 'le', high);

  // Intersection of two ranges
  return Math.max(0, Math.min(lowOverlap, highOverlap));
}

/**
 * Linear interpolation within a bucket
 */
function linearInterpolate(low: SqlValue, high: SqlValue, value: SqlValue): number {
  if (typeof low === 'number' && typeof high === 'number' && typeof value === 'number') {
    if (high === low) return 0.5;
    return (value - low) / (high - low);
  }

  // For non-numeric types, assume uniform distribution
  return 0.5;
}

/**
 * Estimate range selectivity from min/max values
 */
function estimateRangeFromMinMax(
  min: SqlValue,
  max: SqlValue,
  operator: 'lt' | 'le' | 'gt' | 'ge',
  value: SqlValue
): number {
  if (typeof min !== 'number' || typeof max !== 'number' || typeof value !== 'number') {
    return 0.33; // Default for non-numeric types
  }

  if (max === min) return 0.5;

  const fraction = (value - min) / (max - min);
  const clampedFraction = Math.max(0, Math.min(1, fraction));

  switch (operator) {
    case 'lt':
    case 'le':
      return clampedFraction;
    case 'gt':
    case 'ge':
      return 1 - clampedFraction;
    default:
      return 0.33;
  }
}

/**
 * Estimate BETWEEN selectivity from min/max values
 */
function estimateBetweenFromMinMax(
  min: SqlValue,
  max: SqlValue,
  low: SqlValue,
  high: SqlValue
): number {
  if (
    typeof min !== 'number' ||
    typeof max !== 'number' ||
    typeof low !== 'number' ||
    typeof high !== 'number'
  ) {
    return 0.25;
  }

  if (max === min) return 0.5;

  const lowFraction = Math.max(0, Math.min(1, (low - min) / (max - min)));
  const highFraction = Math.max(0, Math.min(1, (high - min) / (max - min)));

  return highFraction - lowFraction;
}

// =============================================================================
// STATISTICS BUILDER
// =============================================================================

/**
 * Builder for creating table statistics
 */
export class TableStatisticsBuilder {
  private stats: TableStatistics;

  constructor(tableName: string) {
    this.stats = {
      tableName,
      rowCount: 0,
      pageCount: 0,
      avgRowSize: 0,
      totalBytes: 0,
      lastAnalyzed: new Date(),
      columns: new Map(),
    };
  }

  setRowCount(count: number): this {
    this.stats.rowCount = count;
    return this;
  }

  setPageCount(count: number): this {
    this.stats.pageCount = count;
    return this;
  }

  setAvgRowSize(size: number): this {
    this.stats.avgRowSize = size;
    return this;
  }

  setTotalBytes(bytes: number): this {
    this.stats.totalBytes = bytes;
    return this;
  }

  addColumn(columnStats: ColumnStatistics): this {
    this.stats.columns.set(columnStats.columnName, columnStats);
    return this;
  }

  build(): TableStatistics {
    return { ...this.stats };
  }
}

/**
 * Builder for creating column statistics
 */
export class ColumnStatisticsBuilder {
  private stats: ColumnStatistics;

  constructor(columnName: string) {
    this.stats = {
      columnName,
      distinctCount: 0,
      nullFraction: 0,
      avgWidth: 0,
    };
  }

  setDistinctCount(count: number): this {
    this.stats.distinctCount = count;
    return this;
  }

  setNullFraction(fraction: number): this {
    this.stats.nullFraction = Math.max(0, Math.min(1, fraction));
    return this;
  }

  setAvgWidth(width: number): this {
    this.stats.avgWidth = width;
    return this;
  }

  setMinMax(min: SqlValue, max: SqlValue): this {
    this.stats.minValue = min;
    this.stats.maxValue = max;
    return this;
  }

  setMostCommonValues(values: MostCommonValue[]): this {
    this.stats.mostCommonValues = values;
    return this;
  }

  setHistogram(buckets: HistogramBucket[]): this {
    this.stats.histogram = buckets;
    return this;
  }

  setCorrelation(correlation: number): this {
    this.stats.correlation = Math.max(-1, Math.min(1, correlation));
    return this;
  }

  build(): ColumnStatistics {
    return { ...this.stats };
  }
}

/**
 * Builder for creating index statistics
 */
export class IndexStatisticsBuilder {
  private stats: IndexStatistics;

  constructor(indexName: string, tableName: string) {
    this.stats = {
      indexName,
      tableName,
      entryCount: 0,
      distinctKeys: 0,
      treeHeight: 1,
      leafPages: 1,
      totalPages: 1,
      avgKeySize: 8,
      clusteringFactor: 1,
      lastAnalyzed: new Date(),
    };
  }

  setEntryCount(count: number): this {
    this.stats.entryCount = count;
    return this;
  }

  setDistinctKeys(count: number): this {
    this.stats.distinctKeys = count;
    return this;
  }

  setTreeHeight(height: number): this {
    this.stats.treeHeight = height;
    return this;
  }

  setLeafPages(pages: number): this {
    this.stats.leafPages = pages;
    return this;
  }

  setTotalPages(pages: number): this {
    this.stats.totalPages = pages;
    return this;
  }

  setAvgKeySize(size: number): this {
    this.stats.avgKeySize = size;
    return this;
  }

  setClusteringFactor(factor: number): this {
    this.stats.clusteringFactor = factor;
    return this;
  }

  addColumnStats(columnStats: IndexColumnStats): this {
    if (!this.stats.columnStats) {
      this.stats.columnStats = new Map();
    }
    this.stats.columnStats.set(columnStats.columnName, columnStats);
    return this;
  }

  build(): IndexStatistics {
    return { ...this.stats };
  }
}

// =============================================================================
// CACHED STATISTICS STORE
// =============================================================================

/**
 * Configuration for statistics caching
 */
export interface StatsCacheConfig {
  /** Time-to-live in milliseconds for cached statistics (default: 5000ms) */
  ttlMs: number;

  /** Maximum number of table entries to cache (default: 1000) */
  maxTableEntries: number;

  /** Maximum number of index entries to cache (default: 2000) */
  maxIndexEntries: number;
}

/**
 * Default statistics cache configuration
 */
export const DEFAULT_STATS_CACHE_CONFIG: StatsCacheConfig = {
  ttlMs: 5000,
  maxTableEntries: 1000,
  maxIndexEntries: 2000,
};

/**
 * Internal cache entry with timestamp for TTL tracking
 */
interface CacheEntry<T> {
  value: T;
  cachedAt: number;
}

/**
 * Statistics cache diagnostic information
 */
export interface StatsCacheDiagnostics {
  /** Number of cached table statistics entries */
  tableEntries: number;

  /** Number of cached index statistics entries */
  indexEntries: number;

  /** Total cache hits */
  hits: number;

  /** Total cache misses */
  misses: number;

  /** Hit rate percentage (0-100) */
  hitRate: number;

  /** TTL in milliseconds */
  ttlMs: number;
}

/**
 * A statistics store wrapper that adds TTL-based caching and DDL invalidation.
 *
 * This reduces planning overhead by caching table row counts, column statistics,
 * and index cardinality with a configurable TTL. DDL operations (ALTER TABLE,
 * CREATE INDEX, DROP TABLE, DROP INDEX) invalidate the relevant cache entries.
 *
 * The cache wraps an underlying StatisticsStore, intercepting read operations
 * with cached results and passing through write operations to the backing store
 * (while also updating the cache).
 */
export class CachedStatisticsStore extends StatisticsStore {
  private readonly tableCache = new Map<string, CacheEntry<TableStatistics>>();
  private readonly indexCache = new Map<string, CacheEntry<IndexStatistics>>();
  private readonly config: StatsCacheConfig;
  private cacheHits = 0;
  private cacheMisses = 0;

  /**
   * A function that provides fresh statistics from the backing store.
   * If not set, the store uses its own internal data (inherited from StatisticsStore).
   */
  private tableStatsProvider?: (tableName: string) => TableStatistics | undefined;
  private indexStatsProvider?: (indexName: string) => IndexStatistics | undefined;

  constructor(config: Partial<StatsCacheConfig> = {}) {
    super();
    this.config = { ...DEFAULT_STATS_CACHE_CONFIG, ...config };
  }

  /**
   * Set a provider function for fetching fresh table statistics.
   * When set, cache misses will call this provider instead of using inherited data.
   */
  setTableStatsProvider(provider: (tableName: string) => TableStatistics | undefined): void {
    this.tableStatsProvider = provider;
  }

  /**
   * Set a provider function for fetching fresh index statistics.
   * When set, cache misses will call this provider instead of using inherited data.
   */
  setIndexStatsProvider(provider: (indexName: string) => IndexStatistics | undefined): void {
    this.indexStatsProvider = provider;
  }

  /**
   * Get statistics for a table, using the cache when available.
   * Returns cached entry if within TTL, otherwise fetches fresh stats.
   */
  override getTableStats(tableName: string): TableStatistics | undefined {
    const now = Date.now();
    const cached = this.tableCache.get(tableName);

    if (cached && (now - cached.cachedAt) < this.config.ttlMs) {
      this.cacheHits++;
      return cached.value;
    }

    this.cacheMisses++;

    // Fetch fresh stats
    const fresh = this.tableStatsProvider
      ? this.tableStatsProvider(tableName)
      : super.getTableStats(tableName);

    if (fresh) {
      this.putTableCache(tableName, fresh, now);
    } else {
      // Remove stale cache entry
      this.tableCache.delete(tableName);
    }

    return fresh;
  }

  /**
   * Get statistics for an index, using the cache when available.
   */
  override getIndexStats(indexName: string): IndexStatistics | undefined {
    const now = Date.now();
    const cached = this.indexCache.get(indexName);

    if (cached && (now - cached.cachedAt) < this.config.ttlMs) {
      this.cacheHits++;
      return cached.value;
    }

    this.cacheMisses++;

    const fresh = this.indexStatsProvider
      ? this.indexStatsProvider(indexName)
      : super.getIndexStats(indexName);

    if (fresh) {
      this.putIndexCache(indexName, fresh, now);
    } else {
      this.indexCache.delete(indexName);
    }

    return fresh;
  }

  /**
   * Set table statistics, updating both the backing store and cache.
   */
  override setTableStats(stats: TableStatistics): void {
    super.setTableStats(stats);
    this.putTableCache(stats.tableName, stats, Date.now());
  }

  /**
   * Set index statistics, updating both the backing store and cache.
   */
  override setIndexStats(stats: IndexStatistics): void {
    super.setIndexStats(stats);
    this.putIndexCache(stats.indexName, stats, Date.now());
  }

  /**
   * Check if statistics exist for a table (checks both cache and backing store).
   */
  override hasTableStats(tableName: string): boolean {
    const now = Date.now();
    const cached = this.tableCache.get(tableName);
    if (cached && (now - cached.cachedAt) < this.config.ttlMs) {
      return true;
    }
    return super.hasTableStats(tableName);
  }

  /**
   * Check if statistics exist for an index (checks both cache and backing store).
   */
  override hasIndexStats(indexName: string): boolean {
    const now = Date.now();
    const cached = this.indexCache.get(indexName);
    if (cached && (now - cached.cachedAt) < this.config.ttlMs) {
      return true;
    }
    return super.hasIndexStats(indexName);
  }

  /**
   * Get row count for a table (with caching).
   */
  override getRowCount(tableName: string, defaultCount = 1000): number {
    return this.getTableStats(tableName)?.rowCount ?? defaultCount;
  }

  /**
   * Get distinct count for a column (with caching).
   */
  override getDistinctCount(tableName: string, columnName: string, defaultCount?: number): number {
    const tableStats = this.getTableStats(tableName);
    if (!tableStats) {
      return defaultCount ?? Math.round(this.getRowCount(tableName) * 0.1);
    }

    const colStats = tableStats.columns.get(columnName);
    if (!colStats) {
      return defaultCount ?? Math.round(tableStats.rowCount * 0.1);
    }

    return colStats.distinctCount;
  }

  /**
   * Get null fraction for a column (with caching).
   */
  override getNullFraction(tableName: string, columnName: string): number {
    const tableStats = this.getTableStats(tableName);
    if (!tableStats) return 0;

    const colStats = tableStats.columns.get(columnName);
    return colStats?.nullFraction ?? 0;
  }

  /**
   * Get histogram for a column (with caching).
   */
  override getHistogram(tableName: string, columnName: string): HistogramBucket[] | undefined {
    const tableStats = this.getTableStats(tableName);
    if (!tableStats) return undefined;

    const colStats = tableStats.columns.get(columnName);
    return colStats?.histogram;
  }

  /**
   * Get most common values for a column (with caching).
   */
  override getMostCommonValues(tableName: string, columnName: string): MostCommonValue[] | undefined {
    const tableStats = this.getTableStats(tableName);
    if (!tableStats) return undefined;

    const colStats = tableStats.columns.get(columnName);
    return colStats?.mostCommonValues;
  }

  /**
   * Invalidate cached statistics for a specific table.
   * Should be called on DDL operations like ALTER TABLE, DROP TABLE.
   */
  invalidateTable(tableName: string): void {
    this.tableCache.delete(tableName);

    // Also invalidate any index stats for this table
    for (const [indexName, entry] of this.indexCache) {
      if (entry.value.tableName === tableName) {
        this.indexCache.delete(indexName);
      }
    }
  }

  /**
   * Invalidate cached statistics for a specific index.
   * Should be called on CREATE INDEX, DROP INDEX.
   */
  invalidateIndex(indexName: string): void {
    this.indexCache.delete(indexName);
  }

  /**
   * Invalidate all cached statistics for a table and its indexes.
   * Convenience method for DDL operations.
   */
  invalidateForDDL(ddlSql: string): void {
    const alterMatch = ddlSql.match(/ALTER\s+TABLE\s+(\w+)/i);
    const dropTableMatch = ddlSql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)/i);
    const createIndexMatch = ddlSql.match(
      /CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(\w+)/i
    );
    const dropIndexMatch = ddlSql.match(/DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?(\w+)(?:\s+ON\s+(\w+))?/i);

    if (alterMatch) {
      this.invalidateTable(alterMatch[1]);
    }
    if (dropTableMatch) {
      this.invalidateTable(dropTableMatch[1]);
    }
    if (createIndexMatch) {
      this.invalidateIndex(createIndexMatch[1]);
      this.invalidateTable(createIndexMatch[2]);
    }
    if (dropIndexMatch) {
      this.invalidateIndex(dropIndexMatch[1]);
      if (dropIndexMatch[2]) {
        this.invalidateTable(dropIndexMatch[2]);
      }
    }
  }

  /**
   * Clear all cached statistics (but not the backing store).
   */
  clearCache(): void {
    this.tableCache.clear();
    this.indexCache.clear();
    this.cacheHits = 0;
    this.cacheMisses = 0;
  }

  /**
   * Clear everything (cache and backing store).
   */
  override clear(): void {
    super.clear();
    this.clearCache();
  }

  /**
   * Get cache diagnostics for monitoring.
   */
  getDiagnostics(): StatsCacheDiagnostics {
    const total = this.cacheHits + this.cacheMisses;
    return {
      tableEntries: this.tableCache.size,
      indexEntries: this.indexCache.size,
      hits: this.cacheHits,
      misses: this.cacheMisses,
      hitRate: total > 0 ? (this.cacheHits / total) * 100 : 0,
      ttlMs: this.config.ttlMs,
    };
  }

  // ============================================================================
  // PRIVATE METHODS
  // ============================================================================

  private putTableCache(tableName: string, stats: TableStatistics, now: number): void {
    // Evict if at capacity
    if (this.tableCache.size >= this.config.maxTableEntries && !this.tableCache.has(tableName)) {
      this.evictOldestTableEntry();
    }
    this.tableCache.set(tableName, { value: stats, cachedAt: now });
  }

  private putIndexCache(indexName: string, stats: IndexStatistics, now: number): void {
    if (this.indexCache.size >= this.config.maxIndexEntries && !this.indexCache.has(indexName)) {
      this.evictOldestIndexEntry();
    }
    this.indexCache.set(indexName, { value: stats, cachedAt: now });
  }

  private evictOldestTableEntry(): void {
    let oldestKey: string | undefined;
    let oldestTime = Infinity;

    for (const [key, entry] of this.tableCache) {
      if (entry.cachedAt < oldestTime) {
        oldestTime = entry.cachedAt;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      this.tableCache.delete(oldestKey);
    }
  }

  private evictOldestIndexEntry(): void {
    let oldestKey: string | undefined;
    let oldestTime = Infinity;

    for (const [key, entry] of this.indexCache) {
      if (entry.cachedAt < oldestTime) {
        oldestTime = entry.cachedAt;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      this.indexCache.delete(oldestKey);
    }
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a new statistics store
 */
export function createStatisticsStore(): StatisticsStore {
  return new StatisticsStore();
}

/**
 * Create a new cached statistics store with TTL-based caching
 */
export function createCachedStatisticsStore(config?: Partial<StatsCacheConfig>): CachedStatisticsStore {
  return new CachedStatisticsStore(config);
}

/**
 * Create a table statistics builder
 */
export function tableStatsBuilder(tableName: string): TableStatisticsBuilder {
  return new TableStatisticsBuilder(tableName);
}

/**
 * Create a column statistics builder
 */
export function columnStatsBuilder(columnName: string): ColumnStatisticsBuilder {
  return new ColumnStatisticsBuilder(columnName);
}

/**
 * Create an index statistics builder
 */
export function indexStatsBuilder(indexName: string, tableName: string): IndexStatisticsBuilder {
  return new IndexStatisticsBuilder(indexName, tableName);
}

/**
 * Build histogram buckets from sorted data
 */
export function buildHistogram(
  sortedValues: SqlValue[],
  numBuckets: number
): HistogramBucket[] {
  if (sortedValues.length === 0) return [];

  const buckets: HistogramBucket[] = [];
  const bucketSize = Math.ceil(sortedValues.length / numBuckets);

  for (let i = 0; i < sortedValues.length; i += bucketSize) {
    const bucketValues = sortedValues.slice(i, Math.min(i + bucketSize, sortedValues.length));
    const distinctValues = new Set(bucketValues.map(v => JSON.stringify(v)));

    if (bucketValues.length > 0) {
      buckets.push({
        low: bucketValues[0],
        high: bucketValues[bucketValues.length - 1],
        count: bucketValues.length,
        distinctCount: distinctValues.size,
      });
    }
  }

  return buckets;
}

/**
 * Calculate most common values from data
 */
export function calculateMostCommonValues(
  values: SqlValue[],
  topN: number = 10
): MostCommonValue[] {
  if (values.length === 0) return [];

  const counts = new Map<string, { value: SqlValue; count: number }>();

  for (const value of values) {
    const key = JSON.stringify(value);
    const existing = counts.get(key);
    if (existing) {
      existing.count++;
    } else {
      counts.set(key, { value, count: 1 });
    }
  }

  const sorted = Array.from(counts.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, topN);

  return sorted.map(({ value, count }) => ({
    value,
    frequency: count / values.length,
  }));
}
