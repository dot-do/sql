/**
 * DoSQL Cost-Based Query Planner - Cost Estimation
 *
 * Provides cost estimation for:
 * - Table scans (sequential and index)
 * - Join operations
 * - Aggregation
 * - Sorting
 * - Filtering
 */

import type { Predicate, Expression, ComparisonOp, SqlValue } from '../engine/types.js';
import type {
  CostModelConfig,
  PlanCost,
  PhysicalPlan,
  PhysicalPlanNode,
  ScanNode,
  FilterNode,
  JoinNode,
  AggregateNode,
  SortNode,
  LimitNode,
  IndexDef,
  AccessMethod,
  JoinAlgorithm,
} from './types.js';
import { DEFAULT_COST_MODEL, emptyCost, combineCosts } from './types.js';
import type {
  StatisticsStore,
  TableStatistics,
  IndexStatistics,
} from './stats.js';
import {
  estimateEqualitySelectivity,
  estimateRangeSelectivity,
  estimateBetweenSelectivity,
  estimateInSelectivity,
  estimateLikeSelectivity,
  estimateIsNullSelectivity,
} from './stats.js';

// =============================================================================
// COST ESTIMATOR
// =============================================================================

/**
 * Cost estimator class
 */
export class CostEstimator {
  private readonly stats: StatisticsStore;
  private readonly config: CostModelConfig;
  private readonly indexDefs: Map<string, IndexDef[]>;

  constructor(
    stats: StatisticsStore,
    indexDefs: Map<string, IndexDef[]> = new Map(),
    config: Partial<CostModelConfig> = {}
  ) {
    this.stats = stats;
    this.indexDefs = indexDefs;
    this.config = { ...DEFAULT_COST_MODEL, ...config };
  }

  /**
   * Estimate cost for a physical plan node
   */
  estimate(plan: PhysicalPlanNode): PlanCost {
    switch (plan.nodeType) {
      case 'scan':
        return this.estimateScan(plan as ScanNode);
      case 'filter':
        return this.estimateFilter(plan as FilterNode);
      case 'join':
        return this.estimateJoin(plan as JoinNode);
      case 'aggregate':
        return this.estimateAggregate(plan as AggregateNode);
      case 'sort':
        return this.estimateSort(plan as SortNode);
      case 'limit':
        return this.estimateLimit(plan as LimitNode);
      case 'project':
        return this.estimateProject(plan);
      case 'distinct':
        return this.estimateDistinct(plan);
      case 'union':
        return this.estimateUnion(plan);
      case 'merge':
        return this.estimateMerge(plan);
      default:
        return emptyCost();
    }
  }

  /**
   * Estimate cost for a table scan
   */
  estimateScan(scan: ScanNode): PlanCost {
    const tableStats = this.stats.getTableStats(scan.table);
    const rowCount = tableStats?.rowCount ?? this.config.defaultRowCount;

    switch (scan.accessMethod) {
      case 'seqScan':
        return this.estimateSeqScan(scan, rowCount, tableStats);
      case 'indexScan':
        return this.estimateIndexScan(scan, rowCount, tableStats);
      case 'indexOnlyScan':
        return this.estimateIndexOnlyScan(scan, rowCount, tableStats);
      case 'bitmapIndexScan':
        return this.estimateBitmapIndexScan(scan, rowCount, tableStats);
      case 'tidScan':
        return this.estimateTidScan(scan);
      default:
        return this.estimateSeqScan(scan, rowCount, tableStats);
    }
  }

  /**
   * Estimate sequential scan cost
   */
  private estimateSeqScan(
    scan: ScanNode,
    rowCount: number,
    tableStats?: TableStatistics
  ): PlanCost {
    const pages = tableStats?.pageCount ?? Math.ceil(rowCount / this.config.tableRowsPerPage);

    // Filter selectivity
    const selectivity = scan.filterCondition
      ? this.estimatePredicateSelectivity(scan.filterCondition, scan.table)
      : 1.0;

    const outputRows = Math.max(1, Math.round(rowCount * selectivity));

    // I/O cost: read all pages sequentially
    const ioCost = pages * this.config.sequentialIOCost;

    // CPU cost: evaluate filter for each row
    const cpuCost = scan.filterCondition
      ? rowCount * this.config.cpuComparisonCost * this.predicateComplexity(scan.filterCondition)
      : rowCount * this.config.cpuComparisonCost;

    const totalCost = ioCost + cpuCost;

    return {
      ioOps: pages,
      estimatedRows: outputRows,
      cpuCost,
      memoryBytes: 0, // Seq scan is streaming
      startupCost: 0,
      totalCost,
      usedStatistics: !!tableStats,
      confidence: tableStats ? 0.8 : 0.5,
    };
  }

  /**
   * Estimate index scan cost
   */
  private estimateIndexScan(
    scan: ScanNode,
    rowCount: number,
    tableStats?: TableStatistics
  ): PlanCost {
    if (!scan.indexName) {
      return this.estimateSeqScan(scan, rowCount, tableStats);
    }

    const indexStats = this.stats.getIndexStats(scan.indexName);

    // Estimate selectivity from index conditions
    const selectivity = scan.indexCondition
      ? this.estimateIndexConditionSelectivity(scan.indexCondition, scan.table)
      : 1.0;

    const estimatedRows = Math.max(1, Math.round(rowCount * selectivity));

    // B-tree traversal cost
    const treeHeight = indexStats?.treeHeight ?? Math.ceil(Math.log2(rowCount + 1));
    const indexTraversalCost = treeHeight * this.config.randomIOCost;

    // Leaf page scans for range queries
    const leafPages = Math.ceil(estimatedRows / this.config.indexRowsPerPage);
    const indexScanCost = leafPages * this.config.sequentialIOCost;

    // Heap fetches (random I/O unless clustered)
    const clusteringFactor = indexStats?.clusteringFactor ?? 0.5;
    const heapFetchCost = estimatedRows * (
      clusteringFactor * this.config.sequentialIOCost +
      (1 - clusteringFactor) * this.config.randomIOCost
    );

    // CPU cost for index comparisons and heap fetches
    const cpuCost = (treeHeight + estimatedRows) * this.config.cpuComparisonCost;

    // Additional filter cost
    const filterCost = scan.filterCondition
      ? estimatedRows * this.config.cpuComparisonCost * this.predicateComplexity(scan.filterCondition)
      : 0;

    const totalCost = indexTraversalCost + indexScanCost + heapFetchCost + cpuCost + filterCost;

    return {
      ioOps: Math.ceil(treeHeight + leafPages + estimatedRows * (1 - clusteringFactor)),
      estimatedRows,
      cpuCost: cpuCost + filterCost,
      memoryBytes: 0,
      startupCost: indexTraversalCost,
      totalCost,
      usedStatistics: !!(tableStats || indexStats),
      confidence: tableStats && indexStats ? 0.9 : 0.6,
    };
  }

  /**
   * Estimate index-only scan cost (covering index)
   */
  private estimateIndexOnlyScan(
    scan: ScanNode,
    rowCount: number,
    tableStats?: TableStatistics
  ): PlanCost {
    if (!scan.indexName) {
      return this.estimateSeqScan(scan, rowCount, tableStats);
    }

    const indexStats = this.stats.getIndexStats(scan.indexName);

    const selectivity = scan.indexCondition
      ? this.estimateIndexConditionSelectivity(scan.indexCondition, scan.table)
      : 1.0;

    const estimatedRows = Math.max(1, Math.round(rowCount * selectivity));

    // B-tree traversal
    const treeHeight = indexStats?.treeHeight ?? Math.ceil(Math.log2(rowCount + 1));
    const indexTraversalCost = treeHeight * this.config.randomIOCost;

    // Leaf page scans (no heap fetch!)
    const leafPages = Math.ceil(estimatedRows / this.config.indexRowsPerPage);
    const indexScanCost = leafPages * this.config.sequentialIOCost;

    // CPU cost
    const cpuCost = (treeHeight + estimatedRows) * this.config.cpuComparisonCost;

    const totalCost = indexTraversalCost + indexScanCost + cpuCost;

    return {
      ioOps: Math.ceil(treeHeight + leafPages),
      estimatedRows,
      cpuCost,
      memoryBytes: 0,
      startupCost: indexTraversalCost,
      totalCost,
      usedStatistics: !!(tableStats || indexStats),
      confidence: tableStats && indexStats ? 0.95 : 0.7,
    };
  }

  /**
   * Estimate bitmap index scan cost
   */
  private estimateBitmapIndexScan(
    scan: ScanNode,
    rowCount: number,
    tableStats?: TableStatistics
  ): PlanCost {
    // Bitmap scans are good for medium selectivity
    const selectivity = scan.indexCondition
      ? this.estimateIndexConditionSelectivity(scan.indexCondition, scan.table)
      : 1.0;

    const estimatedRows = Math.max(1, Math.round(rowCount * selectivity));

    // Build bitmap cost
    const indexStats = this.stats.getIndexStats(scan.indexName ?? '');
    const treeHeight = indexStats?.treeHeight ?? Math.ceil(Math.log2(rowCount + 1));
    const bitmapBuildCost = treeHeight * this.config.randomIOCost;

    // Bitmap memory
    const bitmapMemory = Math.ceil(rowCount / 8);

    // Heap scan using bitmap (sequential within pages, random between)
    const totalPages = tableStats?.pageCount ?? Math.ceil(rowCount / this.config.tableRowsPerPage);
    const pagesHit = Math.min(totalPages, Math.ceil(estimatedRows / this.config.tableRowsPerPage * 2));
    const heapScanCost = pagesHit * this.config.sequentialIOCost;

    const cpuCost = estimatedRows * this.config.cpuComparisonCost;

    const totalCost = bitmapBuildCost + heapScanCost + cpuCost;

    return {
      ioOps: Math.ceil(treeHeight + pagesHit),
      estimatedRows,
      cpuCost,
      memoryBytes: bitmapMemory,
      startupCost: bitmapBuildCost,
      totalCost,
      usedStatistics: !!tableStats,
      confidence: tableStats ? 0.7 : 0.5,
    };
  }

  /**
   * Estimate TID scan cost
   */
  private estimateTidScan(scan: ScanNode): PlanCost {
    // TID scans are single-row lookups
    return {
      ioOps: 1,
      estimatedRows: 1,
      cpuCost: this.config.cpuComparisonCost,
      memoryBytes: 0,
      startupCost: 0,
      totalCost: this.config.randomIOCost + this.config.cpuComparisonCost,
      usedStatistics: false,
      confidence: 1.0,
    };
  }

  /**
   * Estimate filter cost
   */
  estimateFilter(filter: FilterNode): PlanCost {
    // First estimate child cost
    const childCost = filter.children.length > 0
      ? this.estimate(filter.children[0])
      : emptyCost();

    const inputRows = childCost.estimatedRows;
    const tableName = this.getTableName(filter.children[0]);

    const selectivity = tableName
      ? this.estimatePredicateSelectivity(filter.predicate, tableName)
      : 0.5;

    const outputRows = Math.max(1, Math.round(inputRows * selectivity));

    const filterCpuCost = inputRows * this.config.cpuComparisonCost *
      this.predicateComplexity(filter.predicate);

    return {
      ioOps: childCost.ioOps,
      estimatedRows: outputRows,
      cpuCost: childCost.cpuCost + filterCpuCost,
      memoryBytes: childCost.memoryBytes,
      startupCost: childCost.startupCost,
      totalCost: childCost.totalCost + filterCpuCost,
      usedStatistics: childCost.usedStatistics,
      confidence: childCost.confidence * 0.9,
    };
  }

  /**
   * Estimate join cost
   */
  estimateJoin(join: JoinNode): PlanCost {
    if (join.children.length < 2) {
      return emptyCost();
    }

    const leftCost = this.estimate(join.children[0]);
    const rightCost = this.estimate(join.children[1]);

    switch (join.algorithm) {
      case 'nestedLoop':
        return this.estimateNestedLoopJoin(join, leftCost, rightCost);
      case 'hash':
        return this.estimateHashJoin(join, leftCost, rightCost);
      case 'merge':
        return this.estimateMergeJoin(join, leftCost, rightCost);
      case 'indexNested':
        return this.estimateIndexNestedJoin(join, leftCost, rightCost);
      default:
        return this.estimateHashJoin(join, leftCost, rightCost);
    }
  }

  /**
   * Estimate nested loop join cost
   */
  private estimateNestedLoopJoin(
    join: JoinNode,
    leftCost: PlanCost,
    rightCost: PlanCost
  ): PlanCost {
    const leftRows = leftCost.estimatedRows;
    const rightRows = rightCost.estimatedRows;

    // For each left row, scan all right rows
    const ioOps = leftCost.ioOps + leftRows * rightCost.ioOps;
    const cpuCost = leftCost.cpuCost + leftRows * (rightCost.cpuCost + rightRows * this.config.cpuComparisonCost);

    const outputRows = this.estimateJoinCardinality(
      leftRows,
      rightRows,
      join.joinType,
      !!join.condition
    );

    return {
      ioOps,
      estimatedRows: outputRows,
      cpuCost,
      memoryBytes: 0, // Nested loop is streaming
      startupCost: leftCost.startupCost,
      totalCost: leftCost.totalCost + leftRows * rightCost.totalCost,
      usedStatistics: leftCost.usedStatistics && rightCost.usedStatistics,
      confidence: Math.min(leftCost.confidence, rightCost.confidence) * 0.8,
    };
  }

  /**
   * Estimate hash join cost
   */
  private estimateHashJoin(
    join: JoinNode,
    leftCost: PlanCost,
    rightCost: PlanCost
  ): PlanCost {
    const leftRows = leftCost.estimatedRows;
    const rightRows = rightCost.estimatedRows;

    // Build side: smaller relation (usually right)
    const buildRows = join.buildSide === 'left' ? leftRows : rightRows;
    const probeRows = join.buildSide === 'left' ? rightRows : leftRows;

    // Build cost: insert all build rows into hash table
    const buildCost = buildRows * this.config.hashOperationCost;

    // Probe cost: look up each probe row
    const probeCost = probeRows * this.config.hashOperationCost;

    // Memory for hash table
    const hashMemory = buildRows * this.config.avgRowSize;

    // Check if hash table fits in memory
    const spillCost = hashMemory > this.config.hashMemory
      ? (hashMemory / this.config.hashMemory) * this.config.randomIOCost * 2
      : 0;

    const outputRows = this.estimateJoinCardinality(
      leftRows,
      rightRows,
      join.joinType,
      !!join.condition
    );

    const ioOps = leftCost.ioOps + rightCost.ioOps;
    const cpuCost = leftCost.cpuCost + rightCost.cpuCost + buildCost + probeCost;

    return {
      ioOps,
      estimatedRows: outputRows,
      cpuCost,
      memoryBytes: hashMemory,
      startupCost: leftCost.startupCost + (join.buildSide === 'left' ? leftCost.totalCost : rightCost.totalCost),
      totalCost: leftCost.totalCost + rightCost.totalCost + buildCost + probeCost + spillCost,
      usedStatistics: leftCost.usedStatistics && rightCost.usedStatistics,
      confidence: Math.min(leftCost.confidence, rightCost.confidence) * 0.85,
    };
  }

  /**
   * Estimate merge join cost
   */
  private estimateMergeJoin(
    join: JoinNode,
    leftCost: PlanCost,
    rightCost: PlanCost
  ): PlanCost {
    const leftRows = leftCost.estimatedRows;
    const rightRows = rightCost.estimatedRows;

    // Merge join requires sorted input (cost assumed already in children)
    const mergeCost = (leftRows + rightRows) * this.config.cpuComparisonCost;

    const outputRows = this.estimateJoinCardinality(
      leftRows,
      rightRows,
      join.joinType,
      !!join.condition
    );

    return {
      ioOps: leftCost.ioOps + rightCost.ioOps,
      estimatedRows: outputRows,
      cpuCost: leftCost.cpuCost + rightCost.cpuCost + mergeCost,
      memoryBytes: 0, // Merge join is streaming
      startupCost: leftCost.startupCost + rightCost.startupCost,
      totalCost: leftCost.totalCost + rightCost.totalCost + mergeCost,
      usedStatistics: leftCost.usedStatistics && rightCost.usedStatistics,
      confidence: Math.min(leftCost.confidence, rightCost.confidence) * 0.9,
    };
  }

  /**
   * Estimate index nested loop join cost
   */
  private estimateIndexNestedJoin(
    join: JoinNode,
    leftCost: PlanCost,
    rightCost: PlanCost
  ): PlanCost {
    const leftRows = leftCost.estimatedRows;
    const rightRows = rightCost.estimatedRows;

    // For each left row, do an index lookup on right
    const indexStats = join.indexName ? this.stats.getIndexStats(join.indexName) : undefined;
    const treeHeight = indexStats?.treeHeight ?? Math.ceil(Math.log2(rightRows + 1));

    // Cost per index lookup
    const lookupCost = treeHeight * this.config.randomIOCost + this.config.heapFetchCost;

    const outputRows = this.estimateJoinCardinality(
      leftRows,
      rightRows,
      join.joinType,
      !!join.condition
    );

    return {
      ioOps: leftCost.ioOps + leftRows * (treeHeight + 1),
      estimatedRows: outputRows,
      cpuCost: leftCost.cpuCost + leftRows * treeHeight * this.config.cpuComparisonCost,
      memoryBytes: 0,
      startupCost: leftCost.startupCost,
      totalCost: leftCost.totalCost + leftRows * lookupCost,
      usedStatistics: leftCost.usedStatistics,
      confidence: leftCost.confidence * 0.85,
    };
  }

  /**
   * Estimate aggregate cost
   */
  estimateAggregate(agg: AggregateNode): PlanCost {
    const childCost = agg.children.length > 0
      ? this.estimate(agg.children[0])
      : emptyCost();

    const inputRows = childCost.estimatedRows;

    // Estimate output rows (distinct groups)
    let outputRows: number;
    if (agg.groupBy.length === 0) {
      outputRows = 1; // Single aggregate
    } else {
      // Estimate distinct combinations
      outputRows = this.estimateGroupCount(agg.groupBy, inputRows, agg.children[0]);
    }

    let cpuCost: number;
    let memoryBytes: number;

    switch (agg.strategy) {
      case 'plain':
        cpuCost = inputRows * this.config.cpuComparisonCost;
        memoryBytes = agg.aggregates.length * 16; // Accumulator state
        break;

      case 'sorted':
        cpuCost = inputRows * this.config.cpuComparisonCost * agg.groupBy.length;
        memoryBytes = this.config.avgRowSize; // One group at a time
        break;

      case 'hashed':
        cpuCost = inputRows * this.config.hashOperationCost;
        memoryBytes = outputRows * (this.config.avgRowSize + agg.aggregates.length * 16);
        break;
    }

    // Add cost for each aggregate function
    cpuCost += inputRows * agg.aggregates.length * this.config.cpuComparisonCost;

    // HAVING clause cost
    if (agg.having) {
      cpuCost += outputRows * this.config.cpuComparisonCost * this.predicateComplexity(agg.having);
    }

    return {
      ioOps: childCost.ioOps,
      estimatedRows: outputRows,
      cpuCost: childCost.cpuCost + cpuCost,
      memoryBytes: childCost.memoryBytes + memoryBytes,
      startupCost: agg.strategy === 'hashed' ? childCost.totalCost : childCost.startupCost,
      totalCost: childCost.totalCost + cpuCost,
      usedStatistics: childCost.usedStatistics,
      confidence: childCost.confidence * 0.8,
    };
  }

  /**
   * Estimate sort cost
   */
  estimateSort(sort: SortNode): PlanCost {
    const childCost = sort.children.length > 0
      ? this.estimate(sort.children[0])
      : emptyCost();

    const inputRows = childCost.estimatedRows;
    const outputRows = sort.limit ? Math.min(inputRows, sort.limit) : inputRows;

    // Memory needed for sorting
    const sortMemory = inputRows * this.config.avgRowSize;
    const fitsInMemory = sortMemory <= this.config.sortMemory;

    let sortCost: number;
    let ioOps = childCost.ioOps;

    if (sort.method === 'top-n' || (sort.limit && sort.limit < inputRows * 0.1)) {
      // Top-N heapsort: O(n log k) where k is the limit
      const k = sort.limit ?? inputRows;
      sortCost = inputRows * Math.log2(Math.max(2, k)) * this.config.sortComparisonCost;
    } else if (fitsInMemory) {
      // In-memory quicksort: O(n log n)
      sortCost = inputRows * Math.log2(Math.max(2, inputRows)) * this.config.sortComparisonCost;
    } else {
      // External merge sort
      const numPasses = Math.ceil(Math.log2(sortMemory / this.config.sortMemory));
      const pagesPerPass = Math.ceil(inputRows / this.config.tableRowsPerPage);
      ioOps += numPasses * pagesPerPass * 2; // Read and write each pass
      sortCost = inputRows * Math.log2(Math.max(2, inputRows)) * this.config.sortComparisonCost;
    }

    // Startup cost: must read all input before producing first output (unless top-n)
    const startupCost = sort.method === 'top-n'
      ? childCost.startupCost
      : childCost.totalCost;

    return {
      ioOps,
      estimatedRows: outputRows,
      cpuCost: childCost.cpuCost + sortCost,
      memoryBytes: fitsInMemory ? sortMemory : this.config.sortMemory,
      startupCost,
      totalCost: childCost.totalCost + sortCost + (ioOps - childCost.ioOps) * this.config.sequentialIOCost,
      usedStatistics: childCost.usedStatistics,
      confidence: childCost.confidence,
    };
  }

  /**
   * Estimate limit cost
   */
  estimateLimit(limit: LimitNode): PlanCost {
    const childCost = limit.children.length > 0
      ? this.estimate(limit.children[0])
      : emptyCost();

    const inputRows = childCost.estimatedRows;
    const offset = limit.offset ?? 0;
    const outputRows = Math.max(0, Math.min(limit.limit, inputRows - offset));

    // Fraction of child that needs to be processed
    const fraction = inputRows > 0 ? (offset + outputRows) / inputRows : 0;

    return {
      ioOps: Math.ceil(childCost.ioOps * fraction),
      estimatedRows: outputRows,
      cpuCost: childCost.cpuCost * fraction,
      memoryBytes: childCost.memoryBytes,
      startupCost: childCost.startupCost + offset * (childCost.totalCost - childCost.startupCost) / inputRows,
      totalCost: childCost.startupCost + (childCost.totalCost - childCost.startupCost) * fraction,
      usedStatistics: childCost.usedStatistics,
      confidence: childCost.confidence,
    };
  }

  /**
   * Estimate project cost
   */
  estimateProject(project: PhysicalPlanNode): PlanCost {
    const childCost = project.children.length > 0
      ? this.estimate(project.children[0])
      : emptyCost();

    // Project only adds CPU cost for expression evaluation
    const expressionCount = (project as any).expressions?.length ?? project.outputColumns.length;
    const cpuCost = childCost.estimatedRows * expressionCount * this.config.cpuComparisonCost;

    return {
      ...childCost,
      cpuCost: childCost.cpuCost + cpuCost,
      totalCost: childCost.totalCost + cpuCost,
    };
  }

  /**
   * Estimate distinct cost
   */
  estimateDistinct(distinct: PhysicalPlanNode): PlanCost {
    const childCost = distinct.children.length > 0
      ? this.estimate(distinct.children[0])
      : emptyCost();

    const inputRows = childCost.estimatedRows;
    const distinctStrategy = (distinct as any).strategy ?? 'hash';

    // Estimate distinct output rows
    const outputRows = Math.max(1, Math.round(inputRows * 0.5)); // Rough estimate

    let cpuCost: number;
    let memoryBytes: number;

    if (distinctStrategy === 'hash') {
      cpuCost = inputRows * this.config.hashOperationCost;
      memoryBytes = outputRows * this.config.avgRowSize;
    } else {
      // Sort-based distinct
      cpuCost = inputRows * this.config.cpuComparisonCost;
      memoryBytes = this.config.avgRowSize;
    }

    return {
      ioOps: childCost.ioOps,
      estimatedRows: outputRows,
      cpuCost: childCost.cpuCost + cpuCost,
      memoryBytes: childCost.memoryBytes + memoryBytes,
      startupCost: distinctStrategy === 'hash' ? childCost.totalCost : childCost.startupCost,
      totalCost: childCost.totalCost + cpuCost,
      usedStatistics: childCost.usedStatistics,
      confidence: childCost.confidence * 0.7,
    };
  }

  /**
   * Estimate union cost
   */
  estimateUnion(union: PhysicalPlanNode): PlanCost {
    const childCosts = union.children.map(c => this.estimate(c));
    const combined = combineCosts(childCosts);

    const allRows = childCosts.reduce((sum, c) => sum + c.estimatedRows, 0);
    const isUnionAll = (union as any).all ?? false;

    // UNION ALL just concatenates, UNION needs duplicate elimination
    if (isUnionAll) {
      return {
        ...combined,
        estimatedRows: allRows,
      };
    }

    // UNION with duplicate elimination
    const distinctCost = allRows * this.config.hashOperationCost;
    const memoryBytes = allRows * this.config.avgRowSize;

    return {
      ...combined,
      estimatedRows: Math.round(allRows * 0.8), // Assume some duplicates
      cpuCost: combined.cpuCost + distinctCost,
      memoryBytes: combined.memoryBytes + memoryBytes,
      totalCost: combined.totalCost + distinctCost,
    };
  }

  /**
   * Estimate merge cost
   */
  estimateMerge(merge: PhysicalPlanNode): PlanCost {
    const childCosts = merge.children.map(c => this.estimate(c));
    const combined = combineCosts(childCosts);

    const allRows = childCosts.reduce((sum, c) => sum + c.estimatedRows, 0);

    return {
      ...combined,
      estimatedRows: allRows,
    };
  }

  // ==========================================================================
  // HELPER METHODS
  // ==========================================================================

  /**
   * Estimate predicate selectivity
   */
  estimatePredicateSelectivity(predicate: Predicate, tableName: string): number {
    switch (predicate.type) {
      case 'comparison':
        return this.estimateComparisonSelectivity(predicate, tableName);

      case 'logical':
        if (predicate.op === 'and') {
          return predicate.operands.reduce(
            (sel, op) => sel * this.estimatePredicateSelectivity(op, tableName),
            1.0
          );
        } else if (predicate.op === 'or') {
          // P(A OR B) = P(A) + P(B) - P(A AND B)
          // Simplified: assume independence
          let sel = 0;
          for (const op of predicate.operands) {
            sel += this.estimatePredicateSelectivity(op, tableName);
          }
          return Math.min(1, sel);
        } else if (predicate.op === 'not') {
          return 1 - this.estimatePredicateSelectivity(predicate.operands[0], tableName);
        }
        return 0.5;

      case 'between':
        return estimateBetweenSelectivity(
          this.stats,
          tableName,
          this.getColumnName(predicate.expr),
          this.getLiteralValue(predicate.low),
          this.getLiteralValue(predicate.high)
        );

      case 'in':
        if (Array.isArray(predicate.values)) {
          return estimateInSelectivity(
            this.stats,
            tableName,
            this.getColumnName(predicate.expr),
            predicate.values.map(v => this.getLiteralValue(v))
          );
        }
        return 0.1;

      case 'isNull':
        return estimateIsNullSelectivity(
          this.stats,
          tableName,
          this.getColumnName(predicate.expr),
          predicate.isNot
        );

      default:
        return 0.5;
    }
  }

  /**
   * Estimate comparison predicate selectivity
   */
  private estimateComparisonSelectivity(
    predicate: { op: ComparisonOp; left: Expression; right: Expression },
    tableName: string
  ): number {
    const column = this.getColumnName(predicate.left);
    const value = this.getLiteralValue(predicate.right);

    switch (predicate.op) {
      case 'eq':
        return estimateEqualitySelectivity(this.stats, tableName, column, value);

      case 'ne':
        return 1 - estimateEqualitySelectivity(this.stats, tableName, column, value);

      case 'lt':
      case 'le':
      case 'gt':
      case 'ge':
        return estimateRangeSelectivity(this.stats, tableName, column, predicate.op, value);

      case 'like':
        return estimateLikeSelectivity(String(value));

      default:
        return this.config.rangeSelectivity;
    }
  }

  /**
   * Estimate index condition selectivity
   */
  private estimateIndexConditionSelectivity(
    conditions: Array<{ column: string; operator: ComparisonOp; value: SqlValue }>,
    tableName: string
  ): number {
    let selectivity = 1.0;

    for (const cond of conditions) {
      switch (cond.operator) {
        case 'eq':
          selectivity *= estimateEqualitySelectivity(
            this.stats,
            tableName,
            cond.column,
            cond.value
          );
          break;

        case 'lt':
        case 'le':
        case 'gt':
        case 'ge':
          selectivity *= estimateRangeSelectivity(
            this.stats,
            tableName,
            cond.column,
            cond.operator,
            cond.value
          );
          break;

        default:
          selectivity *= 0.33;
      }
    }

    return selectivity;
  }

  /**
   * Calculate predicate complexity (number of comparisons)
   */
  private predicateComplexity(predicate: Predicate): number {
    switch (predicate.type) {
      case 'comparison':
        return 1;
      case 'logical':
        return predicate.operands.reduce(
          (sum, op) => sum + this.predicateComplexity(op),
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

  /**
   * Estimate join cardinality
   */
  private estimateJoinCardinality(
    leftRows: number,
    rightRows: number,
    joinType: JoinNode['joinType'],
    hasCondition: boolean
  ): number {
    if (joinType === 'cross' || !hasCondition) {
      return leftRows * rightRows;
    }

    // Assume 10% match rate for equi-joins
    const matchRate = 0.1;

    switch (joinType) {
      case 'inner':
        return Math.max(1, Math.round(Math.min(leftRows, rightRows) * matchRate));
      case 'left':
        return leftRows;
      case 'right':
        return rightRows;
      case 'full':
        return Math.max(1, Math.round(leftRows + rightRows - Math.min(leftRows, rightRows) * matchRate));
      default:
        return Math.max(1, Math.round(leftRows * rightRows * matchRate));
    }
  }

  /**
   * Estimate number of groups for GROUP BY
   */
  private estimateGroupCount(
    groupBy: Expression[],
    inputRows: number,
    child: PhysicalPlanNode
  ): number {
    const tableName = this.getTableName(child);
    if (!tableName) {
      return Math.max(1, Math.round(Math.sqrt(inputRows)));
    }

    // Multiply distinct counts (with adjustment for correlation)
    let groups = 1;
    for (const expr of groupBy) {
      const column = this.getColumnName(expr);
      const distinctCount = this.stats.getDistinctCount(tableName, column);
      groups *= distinctCount;
    }

    // Adjust for correlation and cap at input rows
    return Math.max(1, Math.min(inputRows, Math.round(groups * 0.8)));
  }

  /**
   * Extract table name from a plan node
   */
  private getTableName(node: PhysicalPlanNode): string | undefined {
    if (node.nodeType === 'scan') {
      return (node as ScanNode).table;
    }
    if (node.children.length > 0) {
      return this.getTableName(node.children[0]);
    }
    return undefined;
  }

  /**
   * Extract column name from expression
   */
  private getColumnName(expr: Expression): string {
    if (expr.type === 'columnRef') {
      return expr.column;
    }
    return 'unknown';
  }

  /**
   * Extract literal value from expression
   */
  private getLiteralValue(expr: Expression): SqlValue {
    if (expr.type === 'literal') {
      return expr.value;
    }
    return null;
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a cost estimator
 */
export function createCostEstimator(
  stats: StatisticsStore,
  indexDefs?: Map<string, IndexDef[]>,
  config?: Partial<CostModelConfig>
): CostEstimator {
  return new CostEstimator(stats, indexDefs, config);
}

/**
 * Compare two costs (returns negative if a < b, positive if a > b)
 */
export function compareCosts(a: PlanCost, b: PlanCost): number {
  return a.totalCost - b.totalCost;
}

/**
 * Check if cost a is better (lower) than cost b
 */
export function isBetterCost(a: PlanCost, b: PlanCost): boolean {
  return a.totalCost < b.totalCost;
}

/**
 * Format cost for display
 */
export function formatCost(cost: PlanCost): string {
  return `cost=${cost.startupCost.toFixed(2)}..${cost.totalCost.toFixed(2)} rows=${cost.estimatedRows}`;
}
