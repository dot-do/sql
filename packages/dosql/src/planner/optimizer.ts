/**
 * DoSQL Cost-Based Query Planner - Optimizer
 *
 * Provides plan enumeration and optimization:
 * - Index selection
 * - Join ordering (dynamic programming)
 * - Access method selection
 * - Predicate pushdown
 * - Plan costing and comparison
 */

import type {
  QueryPlan,
  Predicate,
  Expression,
  ComparisonOp,
} from '../engine/types.js';
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
  IndexCandidate,
  AccessMethod,
  JoinAlgorithm,
  AnalyzedPredicate,
  PredicatePushdown,
  PlanAlternative,
  OptimizationResult,
} from './types.js';
import {
  DEFAULT_COST_MODEL,
  emptyCost,
  nextPlanNodeId,
  createScanNode,
  createJoinNode,
  createFilterNode,
  createSortNode,
  createLimitNode,
  createAggregateNode,
} from './types.js';
import type { StatisticsStore } from './stats.js';
import {
  estimateEqualitySelectivity,
  estimateRangeSelectivity,
  estimateLikeSelectivity,
} from './stats.js';
import { CostEstimator, createCostEstimator, isBetterCost } from './cost.js';

// =============================================================================
// QUERY OPTIMIZER
// =============================================================================

/**
 * Configuration for the optimizer
 */
export interface OptimizerConfig {
  /** Cost model configuration */
  costModel?: Partial<CostModelConfig>;

  /** Maximum number of join orders to consider */
  maxJoinPermutations?: number;

  /** Enable predicate pushdown */
  enablePredicatePushdown?: boolean;

  /** Enable index selection */
  enableIndexSelection?: boolean;

  /** Enable join reordering */
  enableJoinReordering?: boolean;

  /** Geqo threshold (number of tables to use genetic optimizer) */
  geqoThreshold?: number;
}

/**
 * Query optimizer class
 */
export class QueryOptimizer {
  private readonly stats: StatisticsStore;
  private readonly indexDefs: Map<string, IndexDef[]>;
  private readonly costEstimator: CostEstimator;
  private readonly config: Required<OptimizerConfig>;

  constructor(
    stats: StatisticsStore,
    indexDefs: Map<string, IndexDef[]> = new Map(),
    config: OptimizerConfig = {}
  ) {
    this.stats = stats;
    this.indexDefs = indexDefs;
    this.config = {
      costModel: { ...DEFAULT_COST_MODEL, ...config.costModel },
      maxJoinPermutations: config.maxJoinPermutations ?? 10000,
      enablePredicatePushdown: config.enablePredicatePushdown ?? true,
      enableIndexSelection: config.enableIndexSelection ?? true,
      enableJoinReordering: config.enableJoinReordering ?? true,
      geqoThreshold: config.geqoThreshold ?? 12,
    };
    this.costEstimator = createCostEstimator(
      stats,
      indexDefs,
      this.config.costModel
    );
  }

  /**
   * Optimize a logical query plan
   */
  optimize(logicalPlan: QueryPlan): OptimizationResult {
    const startTime = performance.now();
    const alternatives: PlanAlternative[] = [];
    const indexesConsidered: Set<string> = new Set();
    const statisticsUsed: Set<string> = new Set();

    // Convert logical plan to physical plan with optimization
    const physicalPlan = this.optimizeNode(logicalPlan, alternatives, indexesConsidered);

    // Collect statistics used
    this.collectStatisticsUsed(physicalPlan, statisticsUsed);

    // Mark the best plan
    if (alternatives.length > 0) {
      const bestIndex = alternatives.findIndex(a => a.plan === physicalPlan);
      if (bestIndex >= 0) {
        alternatives[bestIndex].chosen = true;
      }
    }

    const endTime = performance.now();

    return {
      bestPlan: physicalPlan,
      alternatives,
      optimizationTimeMs: endTime - startTime,
      statisticsUsed: Array.from(statisticsUsed),
      indexesConsidered: Array.from(indexesConsidered),
    };
  }

  /**
   * Optimize a single plan node
   */
  private optimizeNode(
    node: QueryPlan,
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    switch (node.type) {
      case 'scan':
        return this.optimizeScan(node, alternatives, indexesConsidered);

      case 'filter':
        return this.optimizeFilter(node, alternatives, indexesConsidered);

      case 'join':
        return this.optimizeJoin(node, alternatives, indexesConsidered);

      case 'aggregate':
        return this.optimizeAggregate(node, alternatives, indexesConsidered);

      case 'sort':
        return this.optimizeSort(node, alternatives, indexesConsidered);

      case 'limit':
        return this.optimizeLimit(node, alternatives, indexesConsidered);

      case 'project':
        return this.optimizeProject(node, alternatives, indexesConsidered);

      case 'distinct':
        return this.optimizeDistinct(node, alternatives, indexesConsidered);

      case 'union':
        return this.optimizeUnion(node, alternatives, indexesConsidered);

      case 'merge':
        return this.optimizeMerge(node, alternatives, indexesConsidered);

      case 'indexLookup':
        return this.optimizeIndexLookup(node, alternatives, indexesConsidered);

      default:
        // Pass through unknown node types
        return createScanNode('unknown', ['*']);
    }
  }

  /**
   * Optimize a table scan
   */
  private optimizeScan(
    scan: QueryPlan & { type: 'scan' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const tableIndexes = this.indexDefs.get(scan.table) ?? [];

    // Generate scan alternatives
    const scanAlternatives: PhysicalPlan[] = [];

    // 1. Sequential scan (always an option)
    const seqScan = createScanNode(scan.table, scan.columns, {
      alias: scan.alias,
      accessMethod: 'seqScan',
      filterCondition: scan.predicate,
    });
    seqScan.cost = this.costEstimator.estimate(seqScan);
    scanAlternatives.push(seqScan);

    // 2. Index scans (if indexes available and predicate provided)
    if (this.config.enableIndexSelection && scan.predicate && tableIndexes.length > 0) {
      for (const index of tableIndexes) {
        indexesConsidered.add(index.name);

        const indexCandidate = this.analyzeIndexForPredicate(
          index,
          scan.predicate,
          scan.table,
          scan.columns
        );

        if (indexCandidate && indexCandidate.usableColumns.length > 0) {
          const indexScan = this.createIndexScanNode(
            scan,
            indexCandidate,
            scan.predicate
          );
          indexScan.cost = this.costEstimator.estimate(indexScan);
          scanAlternatives.push(indexScan);
        }
      }
    }

    // Choose best alternative
    let best = scanAlternatives[0];
    for (const alt of scanAlternatives.slice(1)) {
      if (isBetterCost(alt.cost, best.cost)) {
        best = alt;
      }
    }

    // Record alternatives
    for (const alt of scanAlternatives) {
      alternatives.push({
        plan: alt,
        cost: alt.cost,
        reason: `${alt.accessMethod} on ${scan.table}`,
        chosen: alt === best,
      });
    }

    return best;
  }

  /**
   * Optimize a filter
   */
  private optimizeFilter(
    filter: QueryPlan & { type: 'filter' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    // Try to push predicate down if input is a scan
    if (this.config.enablePredicatePushdown && filter.input.type === 'scan') {
      const scanInput = filter.input as QueryPlan & { type: 'scan' };
      const scanWithPredicate: QueryPlan & { type: 'scan' } = {
        ...scanInput,
        predicate: scanInput.predicate
          ? this.combinPredicates(scanInput.predicate, filter.predicate)
          : filter.predicate,
      };
      return this.optimizeScan(scanWithPredicate, alternatives, indexesConsidered);
    }

    // Optimize child first
    const optimizedChild = this.optimizeNode(filter.input, alternatives, indexesConsidered);

    // Create filter node
    const filterNode = createFilterNode(optimizedChild, filter.predicate);
    filterNode.cost = this.costEstimator.estimate(filterNode);

    return filterNode;
  }

  /**
   * Optimize a join
   */
  private optimizeJoin(
    join: QueryPlan & { type: 'join' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    // Optimize children
    const leftOptimized = this.optimizeNode(join.left, alternatives, indexesConsidered);
    const rightOptimized = this.optimizeNode(join.right, alternatives, indexesConsidered);

    // Generate join alternatives with different algorithms
    const joinAlternatives: PhysicalPlan[] = [];

    // 1. Hash join (good for large datasets)
    const hashJoin = createJoinNode(
      leftOptimized,
      rightOptimized,
      join.joinType,
      'hash',
      join.condition,
      { buildSide: leftOptimized.cost.estimatedRows < rightOptimized.cost.estimatedRows ? 'left' : 'right' }
    );
    hashJoin.cost = this.costEstimator.estimate(hashJoin);
    joinAlternatives.push(hashJoin);

    // 2. Nested loop (good for small outer, indexed inner)
    const nestedLoop = createJoinNode(
      leftOptimized,
      rightOptimized,
      join.joinType,
      'nestedLoop',
      join.condition
    );
    nestedLoop.cost = this.costEstimator.estimate(nestedLoop);
    joinAlternatives.push(nestedLoop);

    // 3. Merge join (good for pre-sorted data)
    if (join.condition) {
      const mergeJoin = createJoinNode(
        leftOptimized,
        rightOptimized,
        join.joinType,
        'merge',
        join.condition
      );
      mergeJoin.cost = this.costEstimator.estimate(mergeJoin);
      joinAlternatives.push(mergeJoin);
    }

    // 4. Index nested loop (if inner has usable index)
    const innerTableName = this.getTableName(rightOptimized);
    if (innerTableName && join.condition) {
      const innerIndexes = this.indexDefs.get(innerTableName) ?? [];
      for (const index of innerIndexes) {
        if (this.canUseIndexForJoin(index, join.condition)) {
          const indexNestedJoin = createJoinNode(
            leftOptimized,
            rightOptimized,
            join.joinType,
            'indexNested',
            join.condition,
            { indexName: index.name }
          );
          indexNestedJoin.cost = this.costEstimator.estimate(indexNestedJoin);
          joinAlternatives.push(indexNestedJoin);
          indexesConsidered.add(index.name);
        }
      }
    }

    // Choose best
    let best = joinAlternatives[0];
    for (const alt of joinAlternatives.slice(1)) {
      if (isBetterCost(alt.cost, best.cost)) {
        best = alt;
      }
    }

    // Record alternatives
    for (const alt of joinAlternatives) {
      const jn = alt as JoinNode;
      alternatives.push({
        plan: alt,
        cost: alt.cost,
        reason: `${jn.algorithm} ${jn.joinType} join`,
        chosen: alt === best,
      });
    }

    return best;
  }

  /**
   * Optimize an aggregate
   */
  private optimizeAggregate(
    agg: QueryPlan & { type: 'aggregate' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const optimizedChild = this.optimizeNode(agg.input, alternatives, indexesConsidered);

    const aggNode = createAggregateNode(
      optimizedChild,
      agg.groupBy,
      agg.aggregates.map(a => ({ expr: a.expr, alias: a.alias })),
      {
        having: agg.having,
        strategy: agg.groupBy.length > 0 ? 'hashed' : 'plain',
      }
    );
    aggNode.cost = this.costEstimator.estimate(aggNode);

    return aggNode;
  }

  /**
   * Optimize a sort
   */
  private optimizeSort(
    sort: QueryPlan & { type: 'sort' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const optimizedChild = this.optimizeNode(sort.input, alternatives, indexesConsidered);

    const sortNode = createSortNode(
      optimizedChild,
      sort.orderBy.map(s => ({
        expr: s.expr,
        direction: s.direction,
        nullsFirst: s.nullsFirst,
      })),
      { method: 'quicksort' }
    );
    sortNode.cost = this.costEstimator.estimate(sortNode);

    return sortNode;
  }

  /**
   * Optimize a limit
   */
  private optimizeLimit(
    limit: QueryPlan & { type: 'limit' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const optimizedChild = this.optimizeNode(limit.input, alternatives, indexesConsidered);

    const limitNode = createLimitNode(optimizedChild, limit.limit, limit.offset);
    limitNode.cost = this.costEstimator.estimate(limitNode);

    return limitNode;
  }

  /**
   * Optimize a project
   */
  private optimizeProject(
    project: QueryPlan & { type: 'project' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const optimizedChild = this.optimizeNode(project.input, alternatives, indexesConsidered);
    const outputCols = project.expressions.map(e => e.alias);

    const projectNode: import('./types.js').ProjectNode = {
      id: nextPlanNodeId(),
      nodeType: 'project',
      expressions: project.expressions,
      outputColumns: outputCols,
      cost: emptyCost(),
      children: [optimizedChild],
    };
    projectNode.cost = this.costEstimator.estimateProject(projectNode);
    return projectNode;
  }

  /**
   * Optimize a distinct
   */
  private optimizeDistinct(
    distinct: QueryPlan & { type: 'distinct' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const optimizedChild = this.optimizeNode(distinct.input, alternatives, indexesConsidered);
    const cols = distinct.columns ?? optimizedChild.outputColumns;

    const distinctNode: import('./types.js').DistinctNode = {
      id: nextPlanNodeId(),
      nodeType: 'distinct',
      strategy: 'hash',
      columns: cols,
      outputColumns: cols,
      cost: emptyCost(),
      children: [optimizedChild],
    };
    distinctNode.cost = this.costEstimator.estimateDistinct(distinctNode);
    return distinctNode;
  }

  /**
   * Optimize a union
   */
  private optimizeUnion(
    union: QueryPlan & { type: 'union' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const optimizedChildren = union.inputs.map(input =>
      this.optimizeNode(input, alternatives, indexesConsidered)
    );

    const unionNode: import('./types.js').UnionNode = {
      id: nextPlanNodeId(),
      nodeType: 'union',
      all: union.all,
      outputColumns: optimizedChildren[0]?.outputColumns ?? [],
      cost: emptyCost(),
      children: optimizedChildren,
    };
    unionNode.cost = this.costEstimator.estimateUnion(unionNode);
    return unionNode;
  }

  /**
   * Optimize a merge
   */
  private optimizeMerge(
    merge: QueryPlan & { type: 'merge' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    const optimizedChildren = merge.inputs.map(input =>
      this.optimizeNode(input, alternatives, indexesConsidered)
    );

    const mergeNode: import('./types.js').MergeNode = {
      id: nextPlanNodeId(),
      nodeType: 'merge',
      mergeType: 'append',
      sortKeys: merge.orderBy,
      outputColumns: optimizedChildren[0]?.outputColumns ?? [],
      cost: emptyCost(),
      children: optimizedChildren,
    };
    mergeNode.cost = this.costEstimator.estimateMerge(mergeNode);
    return mergeNode;
  }

  /**
   * Optimize an index lookup (pass through)
   */
  private optimizeIndexLookup(
    lookup: QueryPlan & { type: 'indexLookup' },
    alternatives: PlanAlternative[],
    indexesConsidered: Set<string>
  ): PhysicalPlan {
    indexesConsidered.add(lookup.index);

    const scanNode = createScanNode(lookup.table, lookup.columns, {
      alias: lookup.alias,
      accessMethod: 'indexScan',
      indexName: lookup.index,
      indexCondition: lookup.lookupKey.map((expr, i) => ({
        column: `key_${i}`,
        operator: 'eq' as ComparisonOp,
        value: expr.type === 'literal' ? expr.value : null,
        columnIndex: i,
      })),
    });
    scanNode.cost = this.costEstimator.estimate(scanNode);

    return scanNode;
  }

  // ==========================================================================
  // INDEX SELECTION HELPERS
  // ==========================================================================

  /**
   * Analyze if an index can be used for a predicate
   */
  private analyzeIndexForPredicate(
    index: IndexDef,
    predicate: Predicate,
    tableName: string,
    requiredColumns: string[]
  ): IndexCandidate | null {
    const indexColumns = new Set(index.columns.map(c => c.name));
    const usableColumns: string[] = [];
    let leadingEqualities = 0;
    let selectivity = 1.0;

    // Analyze predicate for indexable conditions
    const conditions = this.flattenAndPredicates(predicate);

    for (const cond of conditions) {
      const column = this.getPredicateColumn(cond);
      if (column && indexColumns.has(column)) {
        usableColumns.push(column);

        // Count leading equalities
        const colIndex = index.columns.findIndex(c => c.name === column);
        if (colIndex === leadingEqualities && this.isEqualityPredicate(cond)) {
          leadingEqualities++;
        }

        // Update selectivity
        selectivity *= this.estimateConditionSelectivity(cond, tableName);
      }
    }

    if (usableColumns.length === 0) {
      return null;
    }

    // Determine access method
    let accessMethod: AccessMethod;
    if (leadingEqualities === index.columns.length) {
      accessMethod = 'indexScan'; // Point lookup
    } else if (leadingEqualities > 0) {
      accessMethod = 'indexScan'; // Prefix scan
    } else if (usableColumns.length > 0) {
      accessMethod = 'bitmapIndexScan'; // Skip scan via bitmap
    } else {
      accessMethod = 'seqScan';
    }

    // Check if covering index
    const allIndexColumns = new Set([
      ...index.columns.map(c => c.name),
      ...(index.include ?? []),
    ]);
    const isCovering = requiredColumns.every(col => allIndexColumns.has(col));

    if (isCovering && accessMethod === 'indexScan') {
      accessMethod = 'indexOnlyScan';
    }

    return {
      index,
      accessType: accessMethod,
      usableColumns,
      leadingEqualities,
      isCovering,
      selectivity,
      cost: emptyCost(), // Will be filled in later
    };
  }

  /**
   * Create an index scan node from a candidate
   */
  private createIndexScanNode(
    scan: QueryPlan & { type: 'scan' },
    candidate: IndexCandidate,
    predicate: Predicate
  ): ScanNode {
    // Extract index conditions from predicate
    const indexConditions = this.extractIndexConditions(predicate, candidate.index);

    // Remaining predicates become filter
    const filterPredicate = this.extractFilterPredicate(predicate, candidate.index);

    return createScanNode(scan.table, scan.columns, {
      alias: scan.alias,
      accessMethod: candidate.accessType,
      indexName: candidate.index.name,
      indexCondition: indexConditions,
      filterCondition: filterPredicate,
    });
  }

  /**
   * Extract index conditions from a predicate
   */
  private extractIndexConditions(
    predicate: Predicate,
    index: IndexDef
  ): ScanNode['indexCondition'] {
    const conditions: NonNullable<ScanNode['indexCondition']> = [];
    const indexColumnSet = new Set(index.columns.map(c => c.name));

    const flatPredicates = this.flattenAndPredicates(predicate);

    for (const pred of flatPredicates) {
      if (pred.type === 'comparison') {
        const column = this.getPredicateColumn(pred);
        if (column && indexColumnSet.has(column)) {
          const colIndex = index.columns.findIndex(c => c.name === column);
          conditions.push({
            column,
            operator: pred.op,
            value: pred.right.type === 'literal' ? pred.right.value : null,
            columnIndex: colIndex,
          });
        }
      }
    }

    // Sort by column index to match index order
    conditions.sort((a, b) => a.columnIndex - b.columnIndex);

    return conditions;
  }

  /**
   * Extract filter predicate (conditions that can't use the index)
   */
  private extractFilterPredicate(
    predicate: Predicate,
    index: IndexDef
  ): Predicate | undefined {
    const indexColumnSet = new Set(index.columns.map(c => c.name));
    const flatPredicates = this.flattenAndPredicates(predicate);

    const filterPredicates = flatPredicates.filter(pred => {
      const column = this.getPredicateColumn(pred);
      return !column || !indexColumnSet.has(column);
    });

    if (filterPredicates.length === 0) {
      return undefined;
    }

    if (filterPredicates.length === 1) {
      return filterPredicates[0];
    }

    return {
      type: 'logical',
      op: 'and',
      operands: filterPredicates,
    };
  }

  /**
   * Check if an index can be used for a join condition
   */
  private canUseIndexForJoin(index: IndexDef, condition: Predicate): boolean {
    if (condition.type !== 'comparison' || condition.op !== 'eq') {
      return false;
    }

    const leftCol = this.getExpressionColumn(condition.left);
    const rightCol = this.getExpressionColumn(condition.right);

    const firstIndexCol = index.columns[0]?.name;

    return leftCol === firstIndexCol || rightCol === firstIndexCol;
  }

  // ==========================================================================
  // PREDICATE HELPERS
  // ==========================================================================

  /**
   * Flatten AND predicates into a list
   */
  private flattenAndPredicates(predicate: Predicate): Predicate[] {
    if (predicate.type === 'logical' && predicate.op === 'and') {
      return predicate.operands.flatMap(op => this.flattenAndPredicates(op));
    }
    return [predicate];
  }

  /**
   * Combine two predicates with AND
   */
  private combinPredicates(a: Predicate, b: Predicate): Predicate {
    return {
      type: 'logical',
      op: 'and',
      operands: [a, b],
    };
  }

  /**
   * Get the column referenced by a predicate (if simple)
   */
  private getPredicateColumn(predicate: Predicate): string | null {
    if (predicate.type === 'comparison') {
      return this.getExpressionColumn(predicate.left);
    }
    if (predicate.type === 'isNull' || predicate.type === 'between' || predicate.type === 'in') {
      return this.getExpressionColumn(predicate.expr);
    }
    return null;
  }

  /**
   * Get column name from expression
   */
  private getExpressionColumn(expr: Expression): string | null {
    if (expr.type === 'columnRef') {
      return expr.column;
    }
    return null;
  }

  /**
   * Check if predicate is an equality condition
   */
  private isEqualityPredicate(predicate: Predicate): boolean {
    return predicate.type === 'comparison' && predicate.op === 'eq';
  }

  /**
   * Estimate selectivity of a single condition
   */
  private estimateConditionSelectivity(predicate: Predicate, tableName: string): number {
    if (predicate.type === 'comparison') {
      const column = this.getExpressionColumn(predicate.left);
      if (!column) return 0.5;

      const value = predicate.right.type === 'literal' ? predicate.right.value : null;

      switch (predicate.op) {
        case 'eq':
          return estimateEqualitySelectivity(this.stats, tableName, column, value);
        case 'lt':
        case 'le':
        case 'gt':
        case 'ge':
          return estimateRangeSelectivity(this.stats, tableName, column, predicate.op, value);
        case 'like':
          return estimateLikeSelectivity(String(value));
        default:
          return 0.33;
      }
    }

    return 0.5;
  }

  // ==========================================================================
  // UTILITY HELPERS
  // ==========================================================================

  /**
   * Get table name from a physical plan node
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
   * Collect statistics used during optimization
   */
  private collectStatisticsUsed(node: PhysicalPlanNode, stats: Set<string>): void {
    if (node.nodeType === 'scan') {
      const scan = node as ScanNode;
      if (this.stats.hasTableStats(scan.table)) {
        stats.add(`table:${scan.table}`);
      }
      if (scan.indexName && this.stats.hasIndexStats(scan.indexName)) {
        stats.add(`index:${scan.indexName}`);
      }
    }

    for (const child of node.children) {
      this.collectStatisticsUsed(child, stats);
    }
  }
}

// =============================================================================
// JOIN ORDERING
// =============================================================================

/**
 * Find optimal join order using dynamic programming
 */
export function findOptimalJoinOrder(
  tables: Array<{ name: string; rowCount: number; plan: PhysicalPlanNode }>,
  joinConditions: Map<string, Map<string, Predicate>>,
  costEstimator: CostEstimator
): PhysicalPlanNode {
  if (tables.length === 0) {
    throw new Error('No tables to join');
  }

  if (tables.length === 1) {
    return tables[0].plan;
  }

  // Dynamic programming: memo[subset] = best plan for joining subset of tables
  const memo = new Map<number, { plan: PhysicalPlanNode; cost: PlanCost }>();

  // Initialize with single tables
  for (let i = 0; i < tables.length; i++) {
    const mask = 1 << i;
    memo.set(mask, { plan: tables[i].plan, cost: tables[i].plan.cost });
  }

  // Build up larger subsets
  for (let size = 2; size <= tables.length; size++) {
    for (const subset of subsetsOfSize(tables.length, size)) {
      let bestPlan: PhysicalPlanNode | null = null;
      let bestCost: PlanCost | null = null;

      // Try all ways to split this subset into two non-empty parts
      for (const [left, right] of splitSubset(subset)) {
        const leftResult = memo.get(left);
        const rightResult = memo.get(right);

        if (!leftResult || !rightResult) continue;

        // Find join condition between left and right
        const condition = findJoinCondition(tables, left, right, joinConditions);

        // Create join node
        const joinNode = createJoinNode(
          leftResult.plan,
          rightResult.plan,
          'inner',
          'hash',
          condition
        );
        joinNode.cost = costEstimator.estimate(joinNode);

        if (!bestCost || isBetterCost(joinNode.cost, bestCost)) {
          bestPlan = joinNode;
          bestCost = joinNode.cost;
        }
      }

      if (bestPlan && bestCost) {
        memo.set(subset, { plan: bestPlan, cost: bestCost });
      }
    }
  }

  // Return the plan for all tables
  const allTablesMask = (1 << tables.length) - 1;
  const result = memo.get(allTablesMask);

  if (!result) {
    throw new Error('Failed to find join order');
  }

  return result.plan;
}

/**
 * Generate all subsets of given size
 */
function* subsetsOfSize(n: number, size: number): Generator<number> {
  function* generate(start: number, current: number, remaining: number): Generator<number> {
    if (remaining === 0) {
      yield current;
      return;
    }

    for (let i = start; i <= n - remaining; i++) {
      yield* generate(i + 1, current | (1 << i), remaining - 1);
    }
  }

  yield* generate(0, 0, size);
}

/**
 * Split a subset into two non-empty parts
 */
function* splitSubset(subset: number): Generator<[number, number]> {
  // Generate all non-empty proper subsets
  for (let left = 1; left < subset; left++) {
    if ((left & subset) === left && left !== subset) {
      const right = subset ^ left;
      if (right > 0 && left < right) {
        yield [left, right];
      }
    }
  }
}

/**
 * Find join condition between two subsets of tables
 */
function findJoinCondition(
  tables: Array<{ name: string }>,
  leftMask: number,
  rightMask: number,
  joinConditions: Map<string, Map<string, Predicate>>
): Predicate | undefined {
  const conditions: Predicate[] = [];

  for (let i = 0; i < tables.length; i++) {
    if (!(leftMask & (1 << i))) continue;
    const leftTable = tables[i].name;

    for (let j = 0; j < tables.length; j++) {
      if (!(rightMask & (1 << j))) continue;
      const rightTable = tables[j].name;

      const cond = joinConditions.get(leftTable)?.get(rightTable) ??
        joinConditions.get(rightTable)?.get(leftTable);

      if (cond) {
        conditions.push(cond);
      }
    }
  }

  if (conditions.length === 0) {
    return undefined;
  }

  if (conditions.length === 1) {
    return conditions[0];
  }

  return {
    type: 'logical',
    op: 'and',
    operands: conditions,
  };
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a query optimizer
 */
export function createQueryOptimizer(
  stats: StatisticsStore,
  indexDefs?: Map<string, IndexDef[]>,
  config?: OptimizerConfig
): QueryOptimizer {
  return new QueryOptimizer(stats, indexDefs, config);
}

/**
 * Suggest join order for a list of tables (greedy algorithm)
 */
export function suggestJoinOrder(
  tables: Array<{ name: string; rowCount: number }>,
  joins: Array<{ left: string; right: string; selectivity?: number }>
): string[] {
  if (tables.length <= 1) {
    return tables.map(t => t.name);
  }

  // Start with smallest table
  const sorted = [...tables].sort((a, b) => a.rowCount - b.rowCount);
  const result: string[] = [sorted[0].name];
  const remaining = new Set(sorted.slice(1).map(t => t.name));

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

    // If no joining table, pick smallest remaining
    if (!bestTable) {
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
