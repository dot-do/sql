/**
 * DoSQL Cost-Based Query Planner - Type Definitions
 *
 * Defines types for:
 * - Plan nodes (physical operations)
 * - Cost estimation
 * - Table and index statistics
 * - Query execution strategies
 */

import type { SqlValue, Expression, Predicate, ComparisonOp } from '../engine/types.js';
import { PlanningContext, getDefaultPlanningContext, resetDefaultPlanningContext } from './planning-context.js';

// =============================================================================
// COST MODEL TYPES
// =============================================================================

/**
 * Cost model parameters for query optimization.
 * These can be tuned based on hardware characteristics.
 */
export interface CostModelConfig {
  /** Cost of a single random I/O operation */
  randomIOCost: number;

  /** Cost of a sequential I/O operation */
  sequentialIOCost: number;

  /** Cost of a single CPU comparison */
  cpuComparisonCost: number;

  /** Cost of fetching a row from the heap */
  heapFetchCost: number;

  /** Cost of a hash operation (build/probe) */
  hashOperationCost: number;

  /** Cost of a sort comparison */
  sortComparisonCost: number;

  /** Memory available for sorting (bytes) */
  sortMemory: number;

  /** Memory available for hash tables (bytes) */
  hashMemory: number;

  /** Default table row count if statistics unavailable */
  defaultRowCount: number;

  /** Default selectivity for equality predicates */
  equalitySelectivity: number;

  /** Default selectivity for range predicates */
  rangeSelectivity: number;

  /** Default selectivity for LIKE predicates */
  likeSelectivity: number;

  /** Rows per index page */
  indexRowsPerPage: number;

  /** Rows per table page */
  tableRowsPerPage: number;

  /** Average row size in bytes */
  avgRowSize: number;
}

/**
 * Default cost model configuration
 */
export const DEFAULT_COST_MODEL: CostModelConfig = {
  randomIOCost: 4.0,
  sequentialIOCost: 1.0,
  cpuComparisonCost: 0.01,
  heapFetchCost: 1.0,
  hashOperationCost: 0.02,
  sortComparisonCost: 0.02,
  sortMemory: 256 * 1024 * 1024, // 256 MB
  hashMemory: 256 * 1024 * 1024, // 256 MB
  defaultRowCount: 1000,
  equalitySelectivity: 0.01,
  rangeSelectivity: 0.33,
  likeSelectivity: 0.10,
  indexRowsPerPage: 100,
  tableRowsPerPage: 50,
  avgRowSize: 100,
};

// =============================================================================
// COST ESTIMATION TYPES
// =============================================================================

/**
 * Detailed cost breakdown for a plan node
 */
export interface PlanCost {
  /** Estimated I/O operations (pages read) */
  ioOps: number;

  /** Estimated number of rows produced */
  estimatedRows: number;

  /** Estimated CPU cost (comparison operations) */
  cpuCost: number;

  /** Memory usage estimate (bytes) */
  memoryBytes: number;

  /** Startup cost (before first row) */
  startupCost: number;

  /** Total cost (full execution) */
  totalCost: number;

  /** Whether statistics were used for this estimate */
  usedStatistics: boolean;

  /** Confidence level (0-1) */
  confidence: number;
}

/**
 * Create a default/empty cost estimate
 */
export function emptyCost(): PlanCost {
  return {
    ioOps: 0,
    estimatedRows: 0,
    cpuCost: 0,
    memoryBytes: 0,
    startupCost: 0,
    totalCost: 0,
    usedStatistics: false,
    confidence: 0,
  };
}

/**
 * Combine costs from multiple sources
 */
export function combineCosts(costs: PlanCost[]): PlanCost {
  return costs.reduce(
    (acc, cost) => ({
      ioOps: acc.ioOps + cost.ioOps,
      estimatedRows: Math.max(acc.estimatedRows, cost.estimatedRows),
      cpuCost: acc.cpuCost + cost.cpuCost,
      memoryBytes: acc.memoryBytes + cost.memoryBytes,
      startupCost: acc.startupCost + cost.startupCost,
      totalCost: acc.totalCost + cost.totalCost,
      usedStatistics: acc.usedStatistics && cost.usedStatistics,
      confidence: Math.min(acc.confidence, cost.confidence),
    }),
    emptyCost()
  );
}

// =============================================================================
// PHYSICAL PLAN NODE TYPES
// =============================================================================

/**
 * Access method for table scans
 */
export type AccessMethod =
  | 'seqScan'           // Sequential table scan
  | 'indexScan'         // Index scan with heap fetch
  | 'indexOnlyScan'     // Index-only scan (covering index)
  | 'bitmapIndexScan'   // Bitmap index scan
  | 'tidScan';          // Direct TID (row ID) scan

/**
 * Join algorithm types
 */
export type JoinAlgorithm =
  | 'nestedLoop'        // Nested loop join
  | 'hash'              // Hash join
  | 'merge'             // Sort-merge join
  | 'indexNested';      // Index nested loop join

/**
 * Aggregate strategy types
 */
export type AggregateStrategy =
  | 'plain'             // Single-group aggregate
  | 'sorted'            // Pre-sorted grouping
  | 'hashed';           // Hash-based grouping

/**
 * Sort method types
 */
export type SortMethod =
  | 'quicksort'         // In-memory quicksort
  | 'external'          // External merge sort
  | 'top-n';            // Top-N heapsort for LIMIT

// =============================================================================
// PHYSICAL PLAN NODES
// =============================================================================

/**
 * Base physical plan node
 */
export interface PhysicalPlanNode {
  /** Unique node ID */
  id: number;

  /** Node type discriminator */
  nodeType: string;

  /** Estimated cost */
  cost: PlanCost;

  /** Output columns */
  outputColumns: string[];

  /** Child nodes */
  children: PhysicalPlanNode[];
}

/**
 * Table scan node
 */
export interface ScanNode extends PhysicalPlanNode {
  nodeType: 'scan';
  table: string;
  alias?: string;
  accessMethod: AccessMethod;
  indexName?: string;
  indexCondition?: IndexCondition[];
  filterCondition?: Predicate;
  projection: string[];
}

/**
 * Index condition for index scans
 */
export interface IndexCondition {
  column: string;
  operator: ComparisonOp;
  value: SqlValue;
  columnIndex: number;
}

/**
 * Filter node
 */
export interface FilterNode extends PhysicalPlanNode {
  nodeType: 'filter';
  predicate: Predicate;
}

/**
 * Project node
 */
export interface ProjectNode extends PhysicalPlanNode {
  nodeType: 'project';
  expressions: Array<{ expr: Expression; alias: string }>;
}

/**
 * Join node
 */
export interface JoinNode extends PhysicalPlanNode {
  nodeType: 'join';
  joinType: 'inner' | 'left' | 'right' | 'full' | 'cross';
  algorithm: JoinAlgorithm;
  condition?: Predicate;
  /** For hash joins: which side builds the hash table */
  buildSide?: 'left' | 'right';
  /** For index nested loop: index to use */
  indexName?: string;
}

/**
 * Aggregate node
 */
export interface AggregateNode extends PhysicalPlanNode {
  nodeType: 'aggregate';
  strategy: AggregateStrategy;
  groupBy: Expression[];
  aggregates: Array<{ expr: Expression; alias: string }>;
  having?: Predicate;
}

/**
 * Sort node
 */
export interface SortNode extends PhysicalPlanNode {
  nodeType: 'sort';
  sortKeys: Array<{
    expr: Expression;
    direction: 'asc' | 'desc';
    nullsFirst?: boolean;
  }>;
  method: SortMethod;
  limit?: number;
}

/**
 * Limit node
 */
export interface LimitNode extends PhysicalPlanNode {
  nodeType: 'limit';
  limit: number;
  offset?: number;
}

/**
 * Distinct node
 */
export interface DistinctNode extends PhysicalPlanNode {
  nodeType: 'distinct';
  strategy: 'sort' | 'hash';
  columns: string[];
}

/**
 * Union node
 */
export interface UnionNode extends PhysicalPlanNode {
  nodeType: 'union';
  all: boolean;
}

/**
 * Merge node (for combining hot/cold data)
 */
export interface MergeNode extends PhysicalPlanNode {
  nodeType: 'merge';
  mergeType: 'append' | 'interleave';
  sortKeys?: Array<{ expr: Expression; direction: 'asc' | 'desc' }>;
}

/**
 * All physical plan node types
 */
export type PhysicalPlan =
  | ScanNode
  | FilterNode
  | ProjectNode
  | JoinNode
  | AggregateNode
  | SortNode
  | LimitNode
  | DistinctNode
  | UnionNode
  | MergeNode;

// =============================================================================
// INDEX TYPES
// =============================================================================

/**
 * Index definition for the planner
 */
export interface IndexDef {
  /** Index name */
  name: string;

  /** Table name */
  table: string;

  /** Index columns in order */
  columns: Array<{
    name: string;
    direction: 'asc' | 'desc';
    nullsFirst?: boolean;
  }>;

  /** Whether the index enforces uniqueness */
  unique: boolean;

  /** INCLUDE columns (for covering indexes) */
  include?: string[];

  /** Partial index condition */
  where?: Predicate;

  /** Index type */
  type?: 'btree' | 'hash' | 'gin' | 'gist';
}

/**
 * Candidate index for a query
 */
export interface IndexCandidate {
  /** Index definition */
  index: IndexDef;

  /** Access type that would be used */
  accessType: AccessMethod;

  /** Columns that can be used from the index */
  usableColumns: string[];

  /** Number of leading equality columns */
  leadingEqualities: number;

  /** Whether this is a covering index for the query */
  isCovering: boolean;

  /** Estimated selectivity */
  selectivity: number;

  /** Estimated cost to use this index */
  cost: PlanCost;
}

// =============================================================================
// PREDICATE ANALYSIS
// =============================================================================

/**
 * Analyzed predicate for index usage
 */
export interface AnalyzedPredicate {
  /** Original predicate */
  original: Predicate;

  /** Column referenced (for simple predicates) */
  column?: string;

  /** Table referenced */
  table?: string;

  /** Operator */
  operator?: ComparisonOp;

  /** Literal value (if comparing to constant) */
  value?: SqlValue;

  /** Whether this predicate can use an index */
  indexable: boolean;

  /** Estimated selectivity (0-1) */
  selectivity: number;
}

/**
 * Result of predicate pushdown analysis
 */
export interface PredicatePushdown {
  /** Predicates that can be pushed to the index */
  indexPredicates: AnalyzedPredicate[];

  /** Predicates that must be evaluated as filters */
  filterPredicates: AnalyzedPredicate[];

  /** Total selectivity */
  combinedSelectivity: number;
}

// =============================================================================
// QUERY PLAN ALTERNATIVES
// =============================================================================

/**
 * A possible execution plan for a query
 */
export interface PlanAlternative {
  /** Physical plan root */
  plan: PhysicalPlan;

  /** Total estimated cost */
  cost: PlanCost;

  /** Reason this plan was considered */
  reason: string;

  /** Whether this is the chosen plan */
  chosen: boolean;
}

/**
 * Result of query optimization
 */
export interface OptimizationResult {
  /** Best (chosen) plan */
  bestPlan: PhysicalPlan;

  /** All alternatives considered */
  alternatives: PlanAlternative[];

  /** Optimization time (ms) */
  optimizationTimeMs: number;

  /** Statistics used */
  statisticsUsed: string[];

  /** Indexes considered */
  indexesConsidered: string[];
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Generate unique plan node IDs using the default (shared) context.
 * For concurrent planning, use PlanningContext directly.
 *
 * @deprecated For new code, use PlanningContext.nextId() with an isolated context
 */
export function nextPlanNodeId(): number {
  return getDefaultPlanningContext().nextId();
}

/**
 * Reset the global plan node ID counter.
 * This resets the default shared context.
 *
 * @deprecated For new code, use isolated PlanningContext instances
 */
export function resetPlanNodeIds(): void {
  resetDefaultPlanningContext();
}

/**
 * Create a scan node with defaults
 */
export function createScanNode(
  table: string,
  columns: string[],
  options?: Partial<Omit<ScanNode, 'nodeType' | 'id' | 'children'>>
): ScanNode {
  return {
    id: nextPlanNodeId(),
    nodeType: 'scan',
    table,
    accessMethod: options?.accessMethod ?? 'seqScan',
    projection: columns,
    outputColumns: columns,
    cost: options?.cost ?? emptyCost(),
    children: [],
    ...options,
  };
}

/**
 * Create a join node with defaults
 */
export function createJoinNode(
  left: PhysicalPlanNode,
  right: PhysicalPlanNode,
  joinType: JoinNode['joinType'],
  algorithm: JoinAlgorithm,
  condition?: Predicate,
  options?: Partial<Omit<JoinNode, 'nodeType' | 'id' | 'children' | 'joinType' | 'algorithm' | 'condition'>>
): JoinNode {
  const outputColumns = [...left.outputColumns, ...right.outputColumns];
  return {
    id: nextPlanNodeId(),
    nodeType: 'join',
    joinType,
    algorithm,
    condition,
    outputColumns,
    cost: options?.cost ?? emptyCost(),
    children: [left, right],
    ...options,
  };
}

/**
 * Create a filter node
 */
export function createFilterNode(
  input: PhysicalPlanNode,
  predicate: Predicate,
  options?: Partial<Omit<FilterNode, 'nodeType' | 'id' | 'children' | 'predicate'>>
): FilterNode {
  return {
    id: nextPlanNodeId(),
    nodeType: 'filter',
    predicate,
    outputColumns: input.outputColumns,
    cost: options?.cost ?? emptyCost(),
    children: [input],
    ...options,
  };
}

/**
 * Create a sort node
 */
export function createSortNode(
  input: PhysicalPlanNode,
  sortKeys: SortNode['sortKeys'],
  options?: Partial<Omit<SortNode, 'nodeType' | 'id' | 'children' | 'sortKeys'>>
): SortNode {
  return {
    id: nextPlanNodeId(),
    nodeType: 'sort',
    sortKeys,
    method: options?.method ?? 'quicksort',
    outputColumns: input.outputColumns,
    cost: options?.cost ?? emptyCost(),
    children: [input],
    ...options,
  };
}

/**
 * Create a limit node
 */
export function createLimitNode(
  input: PhysicalPlanNode,
  limit: number,
  offset?: number,
  options?: Partial<Omit<LimitNode, 'nodeType' | 'id' | 'children' | 'limit' | 'offset'>>
): LimitNode {
  return {
    id: nextPlanNodeId(),
    nodeType: 'limit',
    limit,
    offset,
    outputColumns: input.outputColumns,
    cost: options?.cost ?? emptyCost(),
    children: [input],
    ...options,
  };
}

/**
 * Create an aggregate node
 */
export function createAggregateNode(
  input: PhysicalPlanNode,
  groupBy: Expression[],
  aggregates: AggregateNode['aggregates'],
  options?: Partial<Omit<AggregateNode, 'nodeType' | 'id' | 'children' | 'groupBy' | 'aggregates'>>
): AggregateNode {
  const outputColumns = [
    ...groupBy.map((e, i) => e.type === 'columnRef' ? e.column : `group_${i}`),
    ...aggregates.map(a => a.alias),
  ];
  return {
    id: nextPlanNodeId(),
    nodeType: 'aggregate',
    strategy: options?.strategy ?? (groupBy.length > 0 ? 'hashed' : 'plain'),
    groupBy,
    aggregates,
    outputColumns,
    cost: options?.cost ?? emptyCost(),
    children: [input],
    ...options,
  };
}
