/**
 * Secondary Index Types for DoSQL
 *
 * Defines core types for secondary indexes including:
 * - Index definitions (schema)
 * - Index entries (stored in B-tree)
 * - Index scan ranges (for range queries)
 * - Cost estimation types (for query optimizer)
 */

import type { SqlValue, Expression, Predicate, ComparisonOp } from '../engine/types.js';

// =============================================================================
// INDEX DEFINITION TYPES
// =============================================================================

/**
 * Defines a secondary index on a table
 */
export interface IndexDefinition {
  /** Unique name for the index */
  name: string;

  /** Table this index belongs to */
  table: string;

  /** Columns included in the index (order matters for composite indexes) */
  columns: IndexColumn[];

  /** Whether this is a unique index (enforces uniqueness constraint) */
  unique: boolean;

  /** Additional columns to include in the index (for covering queries) */
  include?: string[];

  /** Optional WHERE clause for partial index */
  where?: Predicate;
}

/**
 * Column specification within an index
 */
export interface IndexColumn {
  /** Column name */
  name: string;

  /** Sort direction for this column in the index */
  direction: 'asc' | 'desc';

  /** How to handle NULL values */
  nulls?: 'first' | 'last';
}

/**
 * Metadata about an index stored alongside index data
 */
export interface IndexMetadata {
  /** Index definition */
  definition: IndexDefinition;

  /** Number of entries in the index */
  entryCount: number;

  /** Approximate number of distinct key values */
  distinctKeys: number;

  /** Timestamp of last rebuild */
  lastRebuild?: number;

  /** Height of the B-tree */
  height: number;

  /** Statistics for cost estimation */
  statistics: IndexStatistics;
}

/**
 * Statistics about an index for cost estimation
 */
export interface IndexStatistics {
  /** Average key size in bytes */
  avgKeySize: number;

  /** Average row ID size in bytes */
  avgRowIdSize: number;

  /** Total size of index in bytes */
  totalBytes: number;

  /** Number of pages in the index */
  pageCount: number;

  /** Histogram buckets for key distribution (optional) */
  histogram?: HistogramBucket[];
}

/**
 * Histogram bucket for key distribution
 */
export interface HistogramBucket {
  /** Lower bound of the bucket (inclusive) */
  low: SqlValue;

  /** Upper bound of the bucket (exclusive) */
  high: SqlValue;

  /** Number of keys in this bucket */
  count: number;

  /** Number of distinct values in this bucket */
  distinctCount: number;
}

// =============================================================================
// INDEX ENTRY TYPES
// =============================================================================

/**
 * An entry in a secondary index
 *
 * Maps a composite key (one or more column values) to a row ID
 */
export interface IndexEntry {
  /** Composite key values (one per indexed column) */
  key: SqlValue[];

  /** Row ID (primary key of the referenced row) */
  rowId: SqlValue;
}

/**
 * Encoded index key for B-tree storage
 *
 * Keys are encoded to maintain correct sort order for composite keys
 */
export interface EncodedIndexKey {
  /** The encoded bytes */
  bytes: Uint8Array;

  /** Original column values (for debugging) */
  values?: SqlValue[];
}

// =============================================================================
// INDEX SCAN TYPES
// =============================================================================

/**
 * Defines a range to scan in an index
 */
export interface IndexScanRange {
  /** Start key (undefined = scan from beginning) */
  start?: IndexBound;

  /** End key (undefined = scan to end) */
  end?: IndexBound;

  /** Whether to scan in reverse order */
  reverse?: boolean;

  /** Maximum number of entries to scan */
  limit?: number;
}

/**
 * A bound for an index range
 */
export interface IndexBound {
  /** Key values for the bound (partial keys allowed) */
  key: SqlValue[];

  /** Whether the bound is inclusive */
  inclusive: boolean;
}

/**
 * Result of an index range scan
 */
export interface IndexScanResult {
  /** Matching index entries */
  entries: IndexEntry[];

  /** Whether there are more results */
  hasMore: boolean;

  /** Continuation token for pagination */
  cursor?: string;

  /** Number of entries scanned (for statistics) */
  entriesScanned: number;
}

// =============================================================================
// INDEX LOOKUP TYPES
// =============================================================================

/**
 * Specifies how an index can be used for a query
 */
export interface IndexLookupSpec {
  /** The index to use */
  index: IndexDefinition;

  /** How the index will be accessed */
  accessType: IndexAccessType;

  /** Range to scan (for range/prefix access) */
  range?: IndexScanRange;

  /** Exact keys to look up (for point access) */
  lookupKeys?: SqlValue[][];

  /** Columns that must be fetched from the table */
  fetchColumns?: string[];

  /** Whether this is a covering index (no table fetch needed) */
  isCovering: boolean;

  /** Estimated cost of using this index */
  cost: IndexCost;
}

/**
 * How an index is accessed
 */
export type IndexAccessType =
  | 'point'      // Exact key lookup
  | 'range'      // Range scan
  | 'prefix'     // Prefix scan (uses leftmost columns)
  | 'full'       // Full index scan
  | 'skip';      // Skip scan (for non-leftmost column)

/**
 * Cost estimate for an index operation
 */
export interface IndexCost {
  /** Estimated number of I/O operations */
  ioOps: number;

  /** Estimated number of rows returned */
  estimatedRows: number;

  /** Estimated CPU cost (comparison operations) */
  cpuCost: number;

  /** Total cost (weighted combination) */
  totalCost: number;

  /** Whether statistics were used for this estimate */
  usedStatistics: boolean;
}

// =============================================================================
// INDEX PREDICATE ANALYSIS
// =============================================================================

/**
 * Result of analyzing a predicate for index usage
 */
export interface PredicateAnalysis {
  /** Predicates that can be evaluated using the index */
  indexPredicates: IndexPredicate[];

  /** Predicates that must be evaluated as a filter after index scan */
  filterPredicates: Predicate[];

  /** Whether all predicates can use the index */
  isFullyIndexable: boolean;
}

/**
 * A predicate that can be pushed to the index
 */
export interface IndexPredicate {
  /** Column index in the composite key */
  columnIndex: number;

  /** Column name */
  column: string;

  /** Comparison operator */
  operator: ComparisonOp;

  /** Value to compare against */
  value: SqlValue;

  /** Whether this predicate creates an equality condition */
  isEquality: boolean;
}

// =============================================================================
// INDEX MAINTENANCE TYPES
// =============================================================================

/**
 * Describes an operation to maintain an index
 */
export interface IndexMaintenanceOp {
  /** Type of operation */
  type: 'insert' | 'delete' | 'update';

  /** The index to update */
  index: IndexDefinition;

  /** Old entry (for delete/update) */
  oldEntry?: IndexEntry;

  /** New entry (for insert/update) */
  newEntry?: IndexEntry;
}

/**
 * Result of index maintenance operations
 */
export interface IndexMaintenanceResult {
  /** Number of entries inserted */
  inserted: number;

  /** Number of entries deleted */
  deleted: number;

  /** Number of unique constraint violations */
  violations: UniqueViolation[];

  /** Time taken for maintenance (ms) */
  durationMs: number;
}

/**
 * Unique constraint violation error
 */
export interface UniqueViolation {
  /** Index that has the constraint */
  index: string;

  /** Duplicate key values */
  key: SqlValue[];

  /** Existing row ID with this key */
  existingRowId: SqlValue;

  /** New row ID that would be duplicate */
  newRowId: SqlValue;
}

// =============================================================================
// COVERING INDEX DETECTION
// =============================================================================

/**
 * Information about covering index potential
 */
export interface CoveringIndexInfo {
  /** Whether all required columns are in the index */
  isCovering: boolean;

  /** Columns in the index key */
  keyColumns: string[];

  /** Columns in INCLUDE clause */
  includedColumns: string[];

  /** Columns missing from the index */
  missingColumns: string[];
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Create a simple single-column index definition
 */
export function createIndex(
  name: string,
  table: string,
  column: string,
  options?: {
    unique?: boolean;
    direction?: 'asc' | 'desc';
    include?: string[];
  }
): IndexDefinition {
  return {
    name,
    table,
    columns: [{ name: column, direction: options?.direction ?? 'asc' }],
    unique: options?.unique ?? false,
    include: options?.include,
  };
}

/**
 * Create a composite index definition
 */
export function createCompositeIndex(
  name: string,
  table: string,
  columns: Array<string | IndexColumn>,
  options?: {
    unique?: boolean;
    include?: string[];
  }
): IndexDefinition {
  return {
    name,
    table,
    columns: columns.map(col =>
      typeof col === 'string'
        ? { name: col, direction: 'asc' as const }
        : col
    ),
    unique: options?.unique ?? false,
    include: options?.include,
  };
}

/**
 * Create an index scan range for a point lookup
 */
export function pointLookup(key: SqlValue[]): IndexScanRange {
  return {
    start: { key, inclusive: true },
    end: { key, inclusive: true },
  };
}

/**
 * Create an index scan range for a range query
 */
export function rangeScan(
  start?: { key: SqlValue[]; inclusive?: boolean },
  end?: { key: SqlValue[]; inclusive?: boolean }
): IndexScanRange {
  return {
    start: start ? { key: start.key, inclusive: start.inclusive ?? true } : undefined,
    end: end ? { key: end.key, inclusive: end.inclusive ?? false } : undefined,
  };
}

/**
 * Create a prefix scan range (for LIKE 'prefix%' queries)
 */
export function prefixScan(prefix: SqlValue[]): IndexScanRange {
  return {
    start: { key: prefix, inclusive: true },
    // End is determined by incrementing the last byte of the prefix
  };
}

/**
 * Check if an index covers all required columns
 */
export function isIndexCovering(
  index: IndexDefinition,
  requiredColumns: string[]
): CoveringIndexInfo {
  const keyColumns = index.columns.map(c => c.name);
  const includedColumns = index.include ?? [];
  const allIndexColumns = new Set([...keyColumns, ...includedColumns]);

  const missingColumns = requiredColumns.filter(col => !allIndexColumns.has(col));

  return {
    isCovering: missingColumns.length === 0,
    keyColumns,
    includedColumns,
    missingColumns,
  };
}

/**
 * Calculate the number of leading equality columns in a predicate
 */
export function countLeadingEqualities(
  predicates: IndexPredicate[],
  indexColumns: IndexColumn[]
): number {
  let count = 0;
  for (let i = 0; i < indexColumns.length; i++) {
    const colName = indexColumns[i].name;
    const pred = predicates.find(p => p.column === colName && p.isEquality);
    if (pred) {
      count++;
    } else {
      break;
    }
  }
  return count;
}
