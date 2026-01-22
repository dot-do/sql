/**
 * Query Optimizer for DoSQL
 *
 * Performs cost-based optimization to choose between:
 * - Full table scan
 * - Index scan (single or composite)
 * - Index-only scan (covering index)
 * - Join ordering
 *
 * The optimizer analyzes WHERE predicates to determine which indexes can be used.
 */

import type {
  QueryPlan,
  ScanPlan,
  IndexLookupPlan,
  FilterPlan,
  JoinPlan,
  Predicate,
  ComparisonPredicate,
  LogicalPredicate,
  BetweenPredicate,
  InPredicate,
  Expression,
  ColumnRef,
  Literal,
  Schema,
  TableSchema,
} from '../engine/types.js';

import {
  type IndexDefinition,
  type IndexLookupSpec,
  type IndexCost,
  type IndexAccessType,
  type IndexScanRange,
  type PredicateAnalysis,
  type IndexPredicate,
  type CoveringIndexInfo,
  isIndexCovering,
  countLeadingEqualities,
} from './types.js';

import type { SecondaryIndex } from './secondary.js';

// =============================================================================
// COST MODEL CONSTANTS
// =============================================================================

/**
 * Cost model parameters (can be tuned based on hardware)
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

  /** Default table row count if statistics unavailable */
  defaultRowCount: number;

  /** Selectivity for equality predicates */
  equalitySelectivity: number;

  /** Selectivity for range predicates */
  rangeSelectivity: number;

  /** Selectivity for LIKE predicates */
  likeSelectivity: number;

  /** Rows per index page */
  indexRowsPerPage: number;

  /** Rows per table page */
  tableRowsPerPage: number;
}

/**
 * Default cost model configuration
 */
export const DEFAULT_COST_MODEL: CostModelConfig = {
  randomIOCost: 4.0,
  sequentialIOCost: 1.0,
  cpuComparisonCost: 0.01,
  heapFetchCost: 1.0,
  defaultRowCount: 1000,
  equalitySelectivity: 0.01,
  rangeSelectivity: 0.33,
  likeSelectivity: 0.10,
  indexRowsPerPage: 100,
  tableRowsPerPage: 50,
};

// =============================================================================
// PREDICATE ANALYSIS
// =============================================================================

/**
 * Analyze a predicate to extract indexable conditions
 */
export function analyzePredicate(
  predicate: Predicate,
  index: IndexDefinition
): PredicateAnalysis {
  const indexPredicates: IndexPredicate[] = [];
  const filterPredicates: Predicate[] = [];

  const indexColumnNames = new Set(index.columns.map(c => c.name));

  // Flatten AND predicates
  const conditions = flattenAndPredicates(predicate);

  for (const cond of conditions) {
    const indexPred = tryExtractIndexPredicate(cond, index, indexColumnNames);
    if (indexPred) {
      indexPredicates.push(indexPred);
    } else {
      filterPredicates.push(cond);
    }
  }

  // Sort by column index to match index order
  indexPredicates.sort((a, b) => a.columnIndex - b.columnIndex);

  return {
    indexPredicates,
    filterPredicates,
    isFullyIndexable: filterPredicates.length === 0,
  };
}

/**
 * Flatten nested AND predicates into a list
 */
function flattenAndPredicates(predicate: Predicate): Predicate[] {
  if (predicate.type === 'logical' && predicate.op === 'and') {
    return predicate.operands.flatMap(flattenAndPredicates);
  }
  return [predicate];
}

/**
 * Try to extract an index predicate from a condition
 */
function tryExtractIndexPredicate(
  predicate: Predicate,
  index: IndexDefinition,
  indexColumnNames: Set<string>
): IndexPredicate | null {
  if (predicate.type !== 'comparison') {
    return null;
  }

  const comp = predicate as ComparisonPredicate;

  // Check if left side is a column reference to an indexed column
  if (comp.left.type !== 'columnRef') {
    return null;
  }

  const colRef = comp.left as ColumnRef;
  if (!indexColumnNames.has(colRef.column)) {
    return null;
  }

  // Check if right side is a literal
  if (comp.right.type !== 'literal') {
    return null;
  }

  const literal = comp.right as Literal;

  // Find column index in the composite key
  const columnIndex = index.columns.findIndex(c => c.name === colRef.column);
  if (columnIndex === -1) {
    return null;
  }

  // Only equality and simple range operators can use index
  const indexableOps = ['eq', 'lt', 'le', 'gt', 'ge'];
  if (!indexableOps.includes(comp.op)) {
    return null;
  }

  return {
    columnIndex,
    column: colRef.column,
    operator: comp.op,
    value: literal.value,
    isEquality: comp.op === 'eq',
  };
}

// =============================================================================
// INDEX SELECTION
// =============================================================================

/**
 * Options for index selection
 */
export interface IndexSelectionOptions {
  /** Available indexes for the table */
  indexes: IndexDefinition[];

  /** Table statistics (row count, etc.) */
  tableStats?: {
    rowCount: number;
    avgRowSize: number;
  };

  /** Index statistics */
  indexStats?: Map<string, {
    distinctKeys: number;
    entryCount: number;
  }>;

  /** Columns required by the query */
  requiredColumns: string[];

  /** Cost model configuration */
  costModel?: Partial<CostModelConfig>;
}

/**
 * Result of index selection
 */
export interface IndexSelectionResult {
  /** Best access method to use */
  accessMethod: 'tableScan' | 'indexScan' | 'indexOnlyScan';

  /** Selected index (if using index) */
  selectedIndex?: IndexDefinition;

  /** Index lookup specification */
  lookupSpec?: IndexLookupSpec;

  /** Estimated cost */
  cost: IndexCost;

  /** Predicates that can be evaluated using the index */
  indexPredicates: IndexPredicate[];

  /** Predicates that must be evaluated as post-filter */
  filterPredicates: Predicate[];
}

/**
 * Select the best index for a query
 */
export function selectIndex(
  predicate: Predicate | undefined,
  options: IndexSelectionOptions
): IndexSelectionResult {
  const costModel = { ...DEFAULT_COST_MODEL, ...options.costModel };
  const rowCount = options.tableStats?.rowCount ?? costModel.defaultRowCount;

  // Calculate table scan cost
  const tableScanCost = estimateTableScanCost(rowCount, predicate, costModel);

  let bestResult: IndexSelectionResult = {
    accessMethod: 'tableScan',
    cost: tableScanCost,
    indexPredicates: [],
    filterPredicates: predicate ? [predicate] : [],
  };

  if (!predicate || options.indexes.length === 0) {
    return bestResult;
  }

  // Evaluate each index
  for (const index of options.indexes) {
    const analysis = analyzePredicate(predicate, index);

    if (analysis.indexPredicates.length === 0) {
      // Index cannot be used for this predicate
      continue;
    }

    // Determine access type based on predicate analysis
    const accessType = determineAccessType(analysis, index);

    // Estimate index scan cost
    const indexCost = estimateIndexScanCost(
      index,
      analysis,
      accessType,
      rowCount,
      options,
      costModel
    );

    // Check if this is a covering index
    const coveringInfo = isIndexCovering(index, options.requiredColumns);

    // For covering index, no heap fetch needed
    let totalCost = indexCost;
    if (!coveringInfo.isCovering) {
      totalCost = addHeapFetchCost(indexCost, costModel);
    }

    if (totalCost.totalCost < bestResult.cost.totalCost) {
      bestResult = {
        accessMethod: coveringInfo.isCovering ? 'indexOnlyScan' : 'indexScan',
        selectedIndex: index,
        lookupSpec: {
          index,
          accessType,
          isCovering: coveringInfo.isCovering,
          cost: totalCost,
        },
        cost: totalCost,
        indexPredicates: analysis.indexPredicates,
        filterPredicates: analysis.filterPredicates,
      };
    }
  }

  return bestResult;
}

/**
 * Determine how the index will be accessed
 */
function determineAccessType(
  analysis: PredicateAnalysis,
  index: IndexDefinition
): IndexAccessType {
  if (analysis.indexPredicates.length === 0) {
    return 'full';
  }

  // Count leading equality predicates
  const leadingEqualities = countLeadingEqualities(
    analysis.indexPredicates,
    index.columns
  );

  // All columns have equality = point lookup
  if (leadingEqualities === index.columns.length) {
    return 'point';
  }

  // Some leading equalities + range on next column
  if (leadingEqualities > 0) {
    const nextColIndex = leadingEqualities;
    const hasRangeOnNext = analysis.indexPredicates.some(
      p => p.columnIndex === nextColIndex && !p.isEquality
    );
    if (hasRangeOnNext || leadingEqualities < index.columns.length) {
      return 'prefix';
    }
  }

  // First predicate is a range
  if (analysis.indexPredicates[0].columnIndex === 0) {
    return 'range';
  }

  // Predicate on non-leading column (skip scan)
  return 'skip';
}

// =============================================================================
// COST ESTIMATION
// =============================================================================

/**
 * Estimate cost of a full table scan
 */
function estimateTableScanCost(
  rowCount: number,
  predicate: Predicate | undefined,
  costModel: CostModelConfig
): IndexCost {
  const pages = Math.ceil(rowCount / costModel.tableRowsPerPage);
  const ioOps = pages;
  const cpuCost = predicate
    ? rowCount * costModel.cpuComparisonCost * estimatePredicateComplexity(predicate)
    : rowCount * costModel.cpuComparisonCost;

  const totalCost =
    ioOps * costModel.sequentialIOCost +
    cpuCost;

  return {
    ioOps,
    estimatedRows: rowCount,
    cpuCost,
    totalCost,
    usedStatistics: false,
  };
}

/**
 * Estimate cost of an index scan
 */
function estimateIndexScanCost(
  index: IndexDefinition,
  analysis: PredicateAnalysis,
  accessType: IndexAccessType,
  tableRowCount: number,
  options: IndexSelectionOptions,
  costModel: CostModelConfig
): IndexCost {
  // Estimate selectivity
  const selectivity = estimateSelectivity(analysis.indexPredicates, costModel);
  const estimatedRows = Math.max(1, Math.round(tableRowCount * selectivity));

  // Calculate I/O cost based on access type
  let ioOps: number;
  switch (accessType) {
    case 'point':
      // B-tree traversal (log n) + one leaf read
      ioOps = Math.ceil(Math.log2(tableRowCount + 1)) + 1;
      break;
    case 'prefix':
    case 'range':
      // B-tree traversal + sequential leaf reads
      const indexPages = Math.ceil(estimatedRows / costModel.indexRowsPerPage);
      ioOps = Math.ceil(Math.log2(tableRowCount + 1)) + indexPages;
      break;
    case 'skip':
      // Multiple B-tree traversals
      const distinctFirstCol = options.indexStats?.get(index.name)?.distinctKeys ?? 100;
      ioOps = distinctFirstCol * Math.ceil(Math.log2(tableRowCount + 1));
      break;
    case 'full':
      // Full index scan
      const allIndexPages = Math.ceil(tableRowCount / costModel.indexRowsPerPage);
      ioOps = allIndexPages;
      break;
  }

  // CPU cost for key comparisons
  const cpuCost = estimatedRows * costModel.cpuComparisonCost * index.columns.length;

  const totalCost =
    (accessType === 'point' ? ioOps * costModel.randomIOCost : ioOps * costModel.sequentialIOCost) +
    cpuCost;

  return {
    ioOps,
    estimatedRows,
    cpuCost,
    totalCost,
    usedStatistics: !!options.indexStats,
  };
}

/**
 * Add heap fetch cost to index scan cost
 */
function addHeapFetchCost(
  indexCost: IndexCost,
  costModel: CostModelConfig
): IndexCost {
  const heapCost = indexCost.estimatedRows * costModel.heapFetchCost;
  return {
    ...indexCost,
    ioOps: indexCost.ioOps + indexCost.estimatedRows,
    totalCost: indexCost.totalCost + heapCost,
  };
}

/**
 * Estimate selectivity of index predicates
 */
function estimateSelectivity(
  predicates: IndexPredicate[],
  costModel: CostModelConfig
): number {
  if (predicates.length === 0) {
    return 1.0;
  }

  let selectivity = 1.0;

  for (const pred of predicates) {
    if (pred.isEquality) {
      selectivity *= costModel.equalitySelectivity;
    } else {
      selectivity *= costModel.rangeSelectivity;
    }
  }

  return selectivity;
}

/**
 * Estimate complexity of a predicate (number of comparisons)
 */
function estimatePredicateComplexity(predicate: Predicate): number {
  switch (predicate.type) {
    case 'comparison':
      return 1;
    case 'logical':
      return predicate.operands.reduce(
        (sum, op) => sum + estimatePredicateComplexity(op),
        0
      );
    case 'between':
      return 2;
    case 'in':
      return Array.isArray(predicate.values) ? predicate.values.length : 10;
    case 'isNull':
      return 1;
    default:
      return 1;
  }
}

// =============================================================================
// JOIN ORDERING
// =============================================================================

/**
 * Estimate join cardinality
 */
export interface JoinEstimate {
  /** Estimated result rows */
  resultRows: number;

  /** Estimated cost */
  cost: number;
}

/**
 * Estimate the cost and cardinality of a join
 */
export function estimateJoinCost(
  leftRows: number,
  rightRows: number,
  joinType: 'inner' | 'left' | 'right' | 'full' | 'cross',
  hasCondition: boolean,
  costModel: CostModelConfig = DEFAULT_COST_MODEL
): JoinEstimate {
  let resultRows: number;

  if (joinType === 'cross') {
    resultRows = leftRows * rightRows;
  } else if (!hasCondition) {
    // No condition = cross join
    resultRows = leftRows * rightRows;
  } else {
    // Assume 10% match rate for inner join
    const matchRate = 0.1;

    switch (joinType) {
      case 'inner':
        resultRows = Math.min(leftRows, rightRows) * matchRate;
        break;
      case 'left':
        resultRows = leftRows;
        break;
      case 'right':
        resultRows = rightRows;
        break;
      case 'full':
        resultRows = leftRows + rightRows - Math.min(leftRows, rightRows) * matchRate;
        break;
      default:
        resultRows = leftRows * rightRows * matchRate;
    }
  }

  // Hash join cost: read both sides + build hash table + probe
  const buildCost = rightRows * costModel.cpuComparisonCost;
  const probeCost = leftRows * costModel.cpuComparisonCost;
  const cost =
    Math.ceil(leftRows / costModel.tableRowsPerPage) * costModel.sequentialIOCost +
    Math.ceil(rightRows / costModel.tableRowsPerPage) * costModel.sequentialIOCost +
    buildCost +
    probeCost;

  return {
    resultRows: Math.max(1, Math.round(resultRows)),
    cost,
  };
}

/**
 * Suggest optimal join order for multiple tables
 *
 * Uses a greedy algorithm for simplicity. A full implementation
 * would use dynamic programming for optimal ordering.
 */
export function suggestJoinOrder(
  tables: Array<{
    name: string;
    rowCount: number;
  }>,
  joins: Array<{
    left: string;
    right: string;
    selectivity?: number;
  }>
): string[] {
  if (tables.length <= 1) {
    return tables.map(t => t.name);
  }

  // Start with the smallest table
  const sortedTables = [...tables].sort((a, b) => a.rowCount - b.rowCount);
  const result: string[] = [sortedTables[0].name];
  const remaining = new Set(sortedTables.slice(1).map(t => t.name));

  // Greedily add tables that have joins with current result
  while (remaining.size > 0) {
    let bestTable: string | null = null;
    let bestCost = Infinity;

    for (const table of remaining) {
      // Check if this table joins with any table in result
      const hasJoin = joins.some(
        j =>
          (result.includes(j.left) && j.right === table) ||
          (result.includes(j.right) && j.left === table)
      );

      if (hasJoin) {
        const tableInfo = tables.find(t => t.name === table);
        const cost = tableInfo?.rowCount ?? Infinity;
        if (cost < bestCost) {
          bestCost = cost;
          bestTable = table;
        }
      }
    }

    // If no table has a join, just pick the smallest remaining
    if (bestTable === null) {
      const smallest = [...remaining]
        .map(name => ({
          name,
          rows: tables.find(t => t.name === name)?.rowCount ?? Infinity,
        }))
        .sort((a, b) => a.rows - b.rows)[0];
      bestTable = smallest.name;
    }

    result.push(bestTable);
    remaining.delete(bestTable);
  }

  return result;
}

// =============================================================================
// QUERY PLAN OPTIMIZATION
// =============================================================================

/**
 * Options for query plan optimization
 */
export interface OptimizationOptions {
  /** Schema information */
  schema: Schema;

  /** Available indexes */
  indexes: Map<string, IndexDefinition[]>;

  /** Table statistics */
  tableStats?: Map<string, { rowCount: number; avgRowSize: number }>;

  /** Index statistics */
  indexStats?: Map<string, { distinctKeys: number; entryCount: number }>;

  /** Cost model configuration */
  costModel?: Partial<CostModelConfig>;
}

/**
 * Optimize a query plan
 */
export function optimizePlan(
  plan: QueryPlan,
  options: OptimizationOptions
): QueryPlan {
  switch (plan.type) {
    case 'scan':
      return optimizeScan(plan, options);

    case 'filter':
      return optimizeFilter(plan, options);

    case 'join':
      return optimizeJoin(plan, options);

    default:
      // Recursively optimize child plans
      return optimizeChildren(plan, options);
  }
}

/**
 * Optimize a scan plan (potentially convert to index scan)
 */
function optimizeScan(
  plan: ScanPlan,
  options: OptimizationOptions
): QueryPlan {
  if (!plan.predicate) {
    return plan;
  }

  const tableIndexes = options.indexes.get(plan.table) ?? [];
  if (tableIndexes.length === 0) {
    return plan;
  }

  const tableStats = options.tableStats?.get(plan.table);

  const selection = selectIndex(plan.predicate, {
    indexes: tableIndexes,
    tableStats,
    requiredColumns: plan.columns,
    costModel: options.costModel,
  });

  if (selection.accessMethod === 'tableScan') {
    return plan;
  }

  // Convert to index lookup
  const indexLookup: IndexLookupPlan = {
    id: plan.id,
    type: 'indexLookup',
    table: plan.table,
    alias: plan.alias,
    index: selection.selectedIndex!.name,
    lookupKey: selection.indexPredicates.map(p => ({
      type: 'literal',
      value: p.value,
      dataType: 'unknown' as const,
    })),
    columns: plan.columns,
    estimatedRows: selection.cost.estimatedRows,
    estimatedCost: selection.cost.totalCost,
  };

  // If there are filter predicates, wrap in a filter node
  if (selection.filterPredicates.length > 0) {
    const filterPredicate: Predicate =
      selection.filterPredicates.length === 1
        ? selection.filterPredicates[0]
        : {
            type: 'logical',
            op: 'and',
            operands: selection.filterPredicates,
          };

    return {
      id: plan.id + 1000,
      type: 'filter',
      input: indexLookup,
      predicate: filterPredicate,
    };
  }

  return indexLookup;
}

/**
 * Optimize a filter plan (push predicate into scan)
 */
function optimizeFilter(
  plan: FilterPlan,
  options: OptimizationOptions
): QueryPlan {
  // First optimize the child
  const optimizedInput = optimizePlan(plan.input, options);

  // If child is a scan without predicate, try to push filter down
  if (optimizedInput.type === 'scan' && !optimizedInput.predicate) {
    const scanWithPredicate: ScanPlan = {
      ...optimizedInput,
      predicate: plan.predicate,
    };
    return optimizeScan(scanWithPredicate, options);
  }

  return {
    ...plan,
    input: optimizedInput,
  };
}

/**
 * Optimize a join plan
 */
function optimizeJoin(
  plan: JoinPlan,
  options: OptimizationOptions
): QueryPlan {
  // Optimize children first
  const optimizedLeft = optimizePlan(plan.left, options);
  const optimizedRight = optimizePlan(plan.right, options);

  // TODO: Implement join reordering based on cardinality estimates

  return {
    ...plan,
    left: optimizedLeft,
    right: optimizedRight,
  };
}

/**
 * Recursively optimize child plans
 */
function optimizeChildren(
  plan: QueryPlan,
  options: OptimizationOptions
): QueryPlan {
  switch (plan.type) {
    case 'filter':
      return { ...plan, input: optimizePlan(plan.input, options) };
    case 'project':
      return { ...plan, input: optimizePlan(plan.input, options) };
    case 'aggregate':
      return { ...plan, input: optimizePlan(plan.input, options) };
    case 'sort':
      return { ...plan, input: optimizePlan(plan.input, options) };
    case 'limit':
      return { ...plan, input: optimizePlan(plan.input, options) };
    case 'distinct':
      return { ...plan, input: optimizePlan(plan.input, options) };
    case 'union':
      return {
        ...plan,
        inputs: plan.inputs.map(input => optimizePlan(input, options)),
      };
    case 'merge':
      return {
        ...plan,
        inputs: plan.inputs.map(input => optimizePlan(input, options)),
      };
    default:
      return plan;
  }
}

// =============================================================================
// OPTIMIZER CLASS
// =============================================================================

/**
 * Query optimizer class
 */
export class QueryOptimizer {
  private options: OptimizationOptions;

  constructor(options: OptimizationOptions) {
    this.options = options;
  }

  /**
   * Optimize a query plan
   */
  optimize(plan: QueryPlan): QueryPlan {
    return optimizePlan(plan, this.options);
  }

  /**
   * Select the best index for a table scan
   */
  selectIndex(
    table: string,
    predicate: Predicate | undefined,
    requiredColumns: string[]
  ): IndexSelectionResult {
    const tableIndexes = this.options.indexes.get(table) ?? [];
    const tableStats = this.options.tableStats?.get(table);

    return selectIndex(predicate, {
      indexes: tableIndexes,
      tableStats,
      requiredColumns,
      costModel: this.options.costModel,
    });
  }

  /**
   * Estimate the cost of a query plan
   */
  estimateCost(plan: QueryPlan): IndexCost {
    // Simple cost estimation by summing child costs
    const costModel = { ...DEFAULT_COST_MODEL, ...this.options.costModel };

    switch (plan.type) {
      case 'scan': {
        const tableStats = this.options.tableStats?.get(plan.table);
        const rowCount = tableStats?.rowCount ?? costModel.defaultRowCount;
        return estimateTableScanCost(rowCount, plan.predicate, costModel);
      }

      case 'indexLookup': {
        return {
          ioOps: plan.estimatedCost ?? 10,
          estimatedRows: plan.estimatedRows ?? 100,
          cpuCost: (plan.estimatedRows ?? 100) * costModel.cpuComparisonCost,
          totalCost: plan.estimatedCost ?? 10,
          usedStatistics: false,
        };
      }

      case 'filter': {
        const inputCost = this.estimateCost(plan.input);
        const filterCpuCost =
          inputCost.estimatedRows *
          costModel.cpuComparisonCost *
          estimatePredicateComplexity(plan.predicate);
        return {
          ...inputCost,
          cpuCost: inputCost.cpuCost + filterCpuCost,
          totalCost: inputCost.totalCost + filterCpuCost,
        };
      }

      case 'join': {
        const leftCost = this.estimateCost(plan.left);
        const rightCost = this.estimateCost(plan.right);
        const joinEstimate = estimateJoinCost(
          leftCost.estimatedRows,
          rightCost.estimatedRows,
          plan.joinType,
          !!plan.condition,
          costModel
        );
        return {
          ioOps: leftCost.ioOps + rightCost.ioOps,
          estimatedRows: joinEstimate.resultRows,
          cpuCost: leftCost.cpuCost + rightCost.cpuCost + joinEstimate.cost,
          totalCost: leftCost.totalCost + rightCost.totalCost + joinEstimate.cost,
          usedStatistics: leftCost.usedStatistics || rightCost.usedStatistics,
        };
      }

      default:
        // For other plan types, just recurse
        if ('input' in plan) {
          return this.estimateCost((plan as any).input);
        }
        return {
          ioOps: 1,
          estimatedRows: costModel.defaultRowCount,
          cpuCost: costModel.cpuComparisonCost,
          totalCost: 1,
          usedStatistics: false,
        };
    }
  }
}

/**
 * Create a query optimizer
 */
export function createOptimizer(options: OptimizationOptions): QueryOptimizer {
  return new QueryOptimizer(options);
}
