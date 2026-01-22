/**
 * EXPLAIN Output for DoSQL
 *
 * Generates human-readable query plan explanations showing:
 * - Access methods (table scan, index scan, index-only scan)
 * - Which indexes are used
 * - Cost estimates
 * - Row estimates
 * - Predicate pushdown
 */

import type {
  QueryPlan,
  ScanPlan,
  IndexLookupPlan,
  FilterPlan,
  ProjectPlan,
  JoinPlan,
  AggregatePlan,
  SortPlan,
  LimitPlan,
  DistinctPlan,
  UnionPlan,
  MergePlan,
  Predicate,
  Expression,
} from '../engine/types.js';

import type { IndexCost } from './types.js';
import type { QueryOptimizer, IndexSelectionResult } from './optimizer.js';

// =============================================================================
// EXPLAIN FORMAT TYPES
// =============================================================================

/**
 * Format for EXPLAIN output
 */
export type ExplainFormat = 'text' | 'json' | 'yaml' | 'tree';

/**
 * Options for EXPLAIN
 */
export interface ExplainOptions {
  /** Output format */
  format?: ExplainFormat;

  /** Include cost estimates */
  costs?: boolean;

  /** Include row estimates */
  rowEstimates?: boolean;

  /** Include buffer/timing info (for EXPLAIN ANALYZE) */
  analyze?: boolean;

  /** Verbose output (show all details) */
  verbose?: boolean;

  /** Query optimizer for cost estimation */
  optimizer?: QueryOptimizer;
}

/**
 * Single node in the explain tree
 */
export interface ExplainNode {
  /** Node type */
  nodeType: string;

  /** Operation description */
  operation: string;

  /** Relation/table name if applicable */
  relationName?: string;

  /** Index name if applicable */
  indexName?: string;

  /** Alias if applicable */
  alias?: string;

  /** Access method used */
  accessMethod?: string;

  /** Index condition (predicates pushed to index) */
  indexCondition?: string;

  /** Filter condition (evaluated after index) */
  filterCondition?: string;

  /** Cost estimates */
  cost?: {
    startup: number;
    total: number;
  };

  /** Row estimates */
  rows?: number;

  /** Width estimate (bytes per row) */
  width?: number;

  /** Actual rows (for ANALYZE) */
  actualRows?: number;

  /** Actual time (for ANALYZE) */
  actualTime?: {
    startup: number;
    total: number;
  };

  /** Loops (for ANALYZE) */
  loops?: number;

  /** Output columns */
  output?: string[];

  /** Child nodes */
  children?: ExplainNode[];

  /** Additional properties */
  properties?: Record<string, string | number | boolean>;
}

// =============================================================================
// EXPLAIN GENERATION
// =============================================================================

/**
 * Generate EXPLAIN output for a query plan
 */
export function explain(
  plan: QueryPlan,
  options: ExplainOptions = {}
): string {
  const format = options.format ?? 'text';
  const node = buildExplainTree(plan, options);

  switch (format) {
    case 'json':
      return JSON.stringify(node, null, 2);
    case 'yaml':
      return formatYaml(node, 0);
    case 'tree':
      return formatTree(node, '', true);
    case 'text':
    default:
      return formatText(node, 0, options);
  }
}

/**
 * Build the explain tree from a query plan
 */
function buildExplainTree(
  plan: QueryPlan,
  options: ExplainOptions
): ExplainNode {
  const estimator = options.optimizer;
  let cost: IndexCost | undefined;

  if (options.costs && estimator) {
    cost = estimator.estimateCost(plan);
  }

  switch (plan.type) {
    case 'scan':
      return buildScanNode(plan, cost, options);

    case 'indexLookup':
      return buildIndexLookupNode(plan, cost, options);

    case 'filter':
      return buildFilterNode(plan, cost, options);

    case 'project':
      return buildProjectNode(plan, cost, options);

    case 'join':
      return buildJoinNode(plan, cost, options);

    case 'aggregate':
      return buildAggregateNode(plan, cost, options);

    case 'sort':
      return buildSortNode(plan, cost, options);

    case 'limit':
      return buildLimitNode(plan, cost, options);

    case 'distinct':
      return buildDistinctNode(plan, cost, options);

    case 'union':
      return buildUnionNode(plan, cost, options);

    case 'merge':
      return buildMergeNode(plan, cost, options);

    default:
      return {
        nodeType: 'Unknown',
        operation: `Unknown plan type: ${(plan as any).type}`,
      };
  }
}

/**
 * Build explain node for table scan
 */
function buildScanNode(
  plan: ScanPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  const accessMethod = plan.source === 'btree'
    ? 'B-Tree Scan'
    : plan.source === 'columnar'
    ? 'Columnar Scan'
    : 'Hybrid Scan';

  return {
    nodeType: 'Seq Scan',
    operation: `Sequential Scan on ${plan.table}`,
    relationName: plan.table,
    alias: plan.alias,
    accessMethod,
    filterCondition: plan.predicate ? formatPredicate(plan.predicate) : undefined,
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    output: plan.columns,
    properties: {
      source: plan.source,
    },
  };
}

/**
 * Build explain node for index lookup
 */
function buildIndexLookupNode(
  plan: IndexLookupPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  const keyStr = plan.lookupKey
    .map((expr, i) => `${i}: ${formatExpression(expr)}`)
    .join(', ');

  return {
    nodeType: 'Index Scan',
    operation: `Index Scan using ${plan.index} on ${plan.table}`,
    relationName: plan.table,
    indexName: plan.index,
    alias: plan.alias,
    accessMethod: 'Index Scan',
    indexCondition: keyStr,
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: plan.estimatedRows ?? cost?.estimatedRows,
    output: plan.columns,
  };
}

/**
 * Build explain node for filter
 */
function buildFilterNode(
  plan: FilterPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  return {
    nodeType: 'Filter',
    operation: 'Filter',
    filterCondition: formatPredicate(plan.predicate),
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    children: [buildExplainTree(plan.input, options)],
  };
}

/**
 * Build explain node for project
 */
function buildProjectNode(
  plan: ProjectPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  return {
    nodeType: 'Project',
    operation: 'Projection',
    output: plan.expressions.map(e => e.alias),
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    children: [buildExplainTree(plan.input, options)],
  };
}

/**
 * Build explain node for join
 */
function buildJoinNode(
  plan: JoinPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  const algorithmName = {
    nestedLoop: 'Nested Loop',
    hash: 'Hash Join',
    merge: 'Merge Join',
  }[plan.algorithm ?? 'hash'];

  const joinTypeName = {
    inner: 'Inner',
    left: 'Left Outer',
    right: 'Right Outer',
    full: 'Full Outer',
    cross: 'Cross',
  }[plan.joinType];

  return {
    nodeType: algorithmName,
    operation: `${algorithmName} ${joinTypeName}`,
    filterCondition: plan.condition ? formatPredicate(plan.condition) : undefined,
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    properties: {
      joinType: plan.joinType,
      algorithm: plan.algorithm ?? 'hash',
    },
    children: [
      buildExplainTree(plan.left, options),
      buildExplainTree(plan.right, options),
    ],
  };
}

/**
 * Build explain node for aggregate
 */
function buildAggregateNode(
  plan: AggregatePlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  const aggregateNames = plan.aggregates.map(a =>
    `${a.expr.function}(${a.expr.arg === '*' ? '*' : formatExpression(a.expr.arg)}) AS ${a.alias}`
  );

  const groupByStr = plan.groupBy.length > 0
    ? plan.groupBy.map(formatExpression).join(', ')
    : undefined;

  return {
    nodeType: plan.groupBy.length > 0 ? 'HashAggregate' : 'Aggregate',
    operation: plan.groupBy.length > 0
      ? `HashAggregate (Group By: ${groupByStr})`
      : 'Aggregate',
    output: aggregateNames,
    filterCondition: plan.having ? formatPredicate(plan.having) : undefined,
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    properties: {
      groupByColumns: plan.groupBy.length,
      aggregateFunctions: plan.aggregates.length,
    },
    children: [buildExplainTree(plan.input, options)],
  };
}

/**
 * Build explain node for sort
 */
function buildSortNode(
  plan: SortPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  const sortKeyStr = plan.orderBy
    .map(s => `${formatExpression(s.expr)} ${s.direction.toUpperCase()}`)
    .join(', ');

  return {
    nodeType: 'Sort',
    operation: `Sort (${sortKeyStr})`,
    cost: cost ? { startup: cost.totalCost * 0.8, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    properties: {
      sortKeys: plan.orderBy.length,
    },
    children: [buildExplainTree(plan.input, options)],
  };
}

/**
 * Build explain node for limit
 */
function buildLimitNode(
  plan: LimitPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  return {
    nodeType: 'Limit',
    operation: `Limit (${plan.limit}${plan.offset ? ` OFFSET ${plan.offset}` : ''})`,
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: Math.min(cost?.estimatedRows ?? plan.limit, plan.limit),
    properties: {
      limit: plan.limit,
      offset: plan.offset ?? 0,
    },
    children: [buildExplainTree(plan.input, options)],
  };
}

/**
 * Build explain node for distinct
 */
function buildDistinctNode(
  plan: DistinctPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  return {
    nodeType: 'Unique',
    operation: 'Unique',
    output: plan.columns,
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    children: [buildExplainTree(plan.input, options)],
  };
}

/**
 * Build explain node for union
 */
function buildUnionNode(
  plan: UnionPlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  return {
    nodeType: plan.all ? 'Append' : 'HashSetOp Union',
    operation: plan.all ? 'UNION ALL' : 'UNION',
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    children: plan.inputs.map(input => buildExplainTree(input, options)),
  };
}

/**
 * Build explain node for merge
 */
function buildMergeNode(
  plan: MergePlan,
  cost: IndexCost | undefined,
  options: ExplainOptions
): ExplainNode {
  return {
    nodeType: 'Merge',
    operation: plan.orderBy
      ? `Merge (ordered by ${plan.orderBy.map(s => formatExpression(s.expr)).join(', ')})`
      : 'Merge (unordered)',
    cost: cost ? { startup: 0, total: cost.totalCost } : undefined,
    rows: cost?.estimatedRows,
    children: plan.inputs.map(input => buildExplainTree(input, options)),
  };
}

// =============================================================================
// FORMATTING UTILITIES
// =============================================================================

/**
 * Format a predicate as a string
 */
function formatPredicate(pred: Predicate): string {
  switch (pred.type) {
    case 'comparison':
      const opStr = {
        eq: '=',
        ne: '<>',
        lt: '<',
        le: '<=',
        gt: '>',
        ge: '>=',
        like: 'LIKE',
        in: 'IN',
        between: 'BETWEEN',
        isNull: 'IS NULL',
        isNotNull: 'IS NOT NULL',
      }[pred.op] ?? pred.op;
      return `${formatExpression(pred.left)} ${opStr} ${formatExpression(pred.right)}`;

    case 'logical':
      if (pred.op === 'not') {
        return `NOT (${formatPredicate(pred.operands[0])})`;
      }
      const sep = pred.op === 'and' ? ' AND ' : ' OR ';
      return `(${pred.operands.map(formatPredicate).join(sep)})`;

    case 'between':
      return `${formatExpression(pred.expr)} BETWEEN ${formatExpression(pred.low)} AND ${formatExpression(pred.high)}`;

    case 'in':
      if (Array.isArray(pred.values)) {
        return `${formatExpression(pred.expr)} IN (${pred.values.map(formatExpression).join(', ')})`;
      }
      return `${formatExpression(pred.expr)} IN (subquery)`;

    case 'isNull':
      return `${formatExpression(pred.expr)} IS ${pred.isNot ? 'NOT ' : ''}NULL`;

    default:
      return 'unknown predicate';
  }
}

/**
 * Format an expression as a string
 */
function formatExpression(expr: Expression): string {
  switch (expr.type) {
    case 'columnRef':
      return expr.table
        ? `${expr.table}.${expr.column}`
        : expr.column;

    case 'literal':
      if (expr.value === null) return 'NULL';
      if (typeof expr.value === 'string') return `'${expr.value}'`;
      if (expr.value instanceof Date) return `'${expr.value.toISOString()}'`;
      return String(expr.value);

    case 'binary':
      const opStr = {
        add: '+', sub: '-', mul: '*', div: '/', mod: '%',
        eq: '=', ne: '<>', lt: '<', le: '<=', gt: '>', ge: '>=',
        and: 'AND', or: 'OR',
      }[expr.op as string] ?? expr.op;
      return `(${formatExpression(expr.left)} ${opStr} ${formatExpression(expr.right)})`;

    case 'unary':
      if (expr.op === 'neg') return `-${formatExpression(expr.operand)}`;
      if (expr.op === 'not') return `NOT ${formatExpression(expr.operand)}`;
      return `${expr.op} ${formatExpression(expr.operand)}`;

    case 'function':
      return `${expr.name}(${expr.args.map(formatExpression).join(', ')})`;

    case 'aggregate':
      const arg = expr.arg === '*' ? '*' : formatExpression(expr.arg);
      return `${expr.function.toUpperCase()}(${expr.distinct ? 'DISTINCT ' : ''}${arg})`;

    case 'case':
      let caseStr = 'CASE';
      for (const w of expr.when) {
        caseStr += ` WHEN ${formatExpression(w.condition)} THEN ${formatExpression(w.result)}`;
      }
      if (expr.else) {
        caseStr += ` ELSE ${formatExpression(expr.else)}`;
      }
      return caseStr + ' END';

    case 'subquery':
      return '(subquery)';

    default:
      return 'unknown';
  }
}

/**
 * Format explain node as text
 */
function formatText(node: ExplainNode, indent: number, options: ExplainOptions): string {
  const prefix = '  '.repeat(indent);
  let lines: string[] = [];

  // Main line: Node type and operation
  let mainLine = `${prefix}-> ${node.operation}`;

  if (node.alias && node.alias !== node.relationName) {
    mainLine += ` (alias: ${node.alias})`;
  }

  lines.push(mainLine);

  // Cost line
  if (options.costs && node.cost) {
    lines.push(`${prefix}   Cost: ${node.cost.startup.toFixed(2)}..${node.cost.total.toFixed(2)}`);
  }

  // Rows line
  if (options.rowEstimates && node.rows !== undefined) {
    lines.push(`${prefix}   Rows: ${node.rows}`);
  }

  // Index condition
  if (node.indexCondition) {
    lines.push(`${prefix}   Index Cond: ${node.indexCondition}`);
  }

  // Filter condition
  if (node.filterCondition) {
    lines.push(`${prefix}   Filter: ${node.filterCondition}`);
  }

  // Output columns (verbose mode)
  if (options.verbose && node.output && node.output.length > 0) {
    lines.push(`${prefix}   Output: ${node.output.join(', ')}`);
  }

  // Analyze info
  if (options.analyze && node.actualRows !== undefined) {
    const timeStr = node.actualTime
      ? ` (actual time=${node.actualTime.startup.toFixed(3)}..${node.actualTime.total.toFixed(3)} ms)`
      : '';
    lines.push(`${prefix}   Actual: rows=${node.actualRows} loops=${node.loops ?? 1}${timeStr}`);
  }

  // Children
  if (node.children) {
    for (const child of node.children) {
      lines.push(formatText(child, indent + 1, options));
    }
  }

  return lines.join('\n');
}

/**
 * Format explain node as tree (ASCII art)
 */
function formatTree(node: ExplainNode, prefix: string, isLast: boolean): string {
  const lines: string[] = [];
  const connector = isLast ? '`-- ' : '|-- ';
  const childPrefix = prefix + (isLast ? '    ' : '|   ');

  lines.push(`${prefix}${connector}${node.nodeType}: ${node.operation}`);

  if (node.children) {
    for (let i = 0; i < node.children.length; i++) {
      const isChildLast = i === node.children.length - 1;
      lines.push(formatTree(node.children[i], childPrefix, isChildLast));
    }
  }

  return lines.join('\n');
}

/**
 * Format explain node as YAML
 */
function formatYaml(node: ExplainNode, indent: number): string {
  const prefix = '  '.repeat(indent);
  let lines: string[] = [];

  lines.push(`${prefix}- Node Type: "${node.nodeType}"`);
  lines.push(`${prefix}  Operation: "${node.operation}"`);

  if (node.relationName) {
    lines.push(`${prefix}  Relation Name: "${node.relationName}"`);
  }

  if (node.indexName) {
    lines.push(`${prefix}  Index Name: "${node.indexName}"`);
  }

  if (node.accessMethod) {
    lines.push(`${prefix}  Access Method: "${node.accessMethod}"`);
  }

  if (node.indexCondition) {
    lines.push(`${prefix}  Index Condition: "${node.indexCondition}"`);
  }

  if (node.filterCondition) {
    lines.push(`${prefix}  Filter: "${node.filterCondition}"`);
  }

  if (node.cost) {
    lines.push(`${prefix}  Startup Cost: ${node.cost.startup}`);
    lines.push(`${prefix}  Total Cost: ${node.cost.total}`);
  }

  if (node.rows !== undefined) {
    lines.push(`${prefix}  Plan Rows: ${node.rows}`);
  }

  if (node.output && node.output.length > 0) {
    lines.push(`${prefix}  Output:`);
    for (const col of node.output) {
      lines.push(`${prefix}    - "${col}"`);
    }
  }

  if (node.children && node.children.length > 0) {
    lines.push(`${prefix}  Plans:`);
    for (const child of node.children) {
      lines.push(formatYaml(child, indent + 2));
    }
  }

  return lines.join('\n');
}

// =============================================================================
// EXPLAIN ANALYZE
// =============================================================================

/**
 * Execution statistics for EXPLAIN ANALYZE
 */
export interface ExecutionStats {
  /** Actual rows produced */
  actualRows: number;

  /** Startup time (ms) */
  startupTime: number;

  /** Total time (ms) */
  totalTime: number;

  /** Number of loops */
  loops: number;

  /** Child statistics */
  children?: ExecutionStats[];
}

/**
 * Add execution statistics to an explain node
 */
export function addExecutionStats(
  node: ExplainNode,
  stats: ExecutionStats
): ExplainNode {
  return {
    ...node,
    actualRows: stats.actualRows,
    actualTime: {
      startup: stats.startupTime,
      total: stats.totalTime,
    },
    loops: stats.loops,
    children: node.children?.map((child, i) =>
      stats.children?.[i]
        ? addExecutionStats(child, stats.children[i])
        : child
    ),
  };
}

// =============================================================================
// EXPLAIN CLASS
// =============================================================================

/**
 * Explain generator class
 */
export class ExplainGenerator {
  private readonly options: ExplainOptions;

  constructor(options: ExplainOptions = {}) {
    this.options = {
      format: 'text',
      costs: true,
      rowEstimates: true,
      analyze: false,
      verbose: false,
      ...options,
    };
  }

  /**
   * Generate EXPLAIN output for a query plan
   */
  explain(plan: QueryPlan): string {
    return explain(plan, this.options);
  }

  /**
   * Generate EXPLAIN output with execution statistics
   */
  explainAnalyze(plan: QueryPlan, stats: ExecutionStats): string {
    const node = buildExplainTree(plan, this.options);
    const nodeWithStats = addExecutionStats(node, stats);

    const format = this.options.format ?? 'text';
    switch (format) {
      case 'json':
        return JSON.stringify(nodeWithStats, null, 2);
      case 'yaml':
        return formatYaml(nodeWithStats, 0);
      case 'tree':
        return formatTree(nodeWithStats, '', true);
      case 'text':
      default:
        return formatText(nodeWithStats, 0, { ...this.options, analyze: true });
    }
  }
}

/**
 * Create an explain generator
 */
export function createExplainGenerator(options?: ExplainOptions): ExplainGenerator {
  return new ExplainGenerator(options);
}
