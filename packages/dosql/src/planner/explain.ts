/**
 * DoSQL Cost-Based Query Planner - EXPLAIN Output
 *
 * Generates human-readable query plan explanations:
 * - Text format (default)
 * - JSON format
 * - YAML format
 * - Tree format (ASCII art)
 * - EXPLAIN ANALYZE with execution statistics
 */

import type {
  PhysicalPlan,
  PhysicalPlanNode,
  ScanNode,
  FilterNode,
  JoinNode,
  AggregateNode,
  SortNode,
  LimitNode,
  UnionNode,
  PlanCost,
} from './types.js';
import type { Expression, Predicate } from '../engine/types.js';

// =============================================================================
// EXPLAIN OUTPUT TYPES
// =============================================================================

/**
 * Output format for EXPLAIN
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

  /** Include memory usage */
  memory?: boolean;

  /** Indentation for text output */
  indent?: number;
}

/**
 * Default explain options
 */
export const DEFAULT_EXPLAIN_OPTIONS: Required<ExplainOptions> = {
  format: 'text',
  costs: true,
  rowEstimates: true,
  analyze: false,
  verbose: false,
  memory: false,
  indent: 2,
};

/**
 * Single node in the explain output tree
 */
export interface ExplainNode {
  /** Node type (e.g., "Seq Scan", "Hash Join") */
  nodeType: string;

  /** Operation description */
  operation: string;

  /** Relation/table name */
  relationName?: string;

  /** Alias */
  alias?: string;

  /** Index name (for index scans) */
  indexName?: string;

  /** Access method */
  accessMethod?: string;

  /** Index condition */
  indexCondition?: string;

  /** Filter condition */
  filter?: string;

  /** Join condition */
  joinCondition?: string;

  /** Hash condition (for hash joins) */
  hashCondition?: string;

  /** Merge condition (for merge joins) */
  mergeCondition?: string;

  /** Sort keys */
  sortKey?: string;

  /** Group keys */
  groupKey?: string;

  /** Output columns */
  output?: string[];

  /** Cost estimates */
  cost?: {
    startup: number;
    total: number;
  };

  /** Row estimate */
  rows?: number;

  /** Width (bytes per row) */
  width?: number;

  /** Memory usage */
  memory?: number;

  /** Actual rows (for ANALYZE) */
  actualRows?: number;

  /** Actual time (for ANALYZE) */
  actualTime?: {
    startup: number;
    total: number;
  };

  /** Loops (for ANALYZE) */
  loops?: number;

  /** Additional properties */
  properties?: Record<string, string | number | boolean>;

  /** Child nodes */
  children?: ExplainNode[];
}

/**
 * Execution statistics for EXPLAIN ANALYZE
 */
export interface ExecutionStats {
  /** Actual rows produced */
  actualRows: number;

  /** Startup time (ms) */
  startupTimeMs: number;

  /** Total time (ms) */
  totalTimeMs: number;

  /** Number of loops */
  loops: number;

  /** Buffer hits */
  bufferHits?: number;

  /** Buffer misses */
  bufferMisses?: number;

  /** Child statistics */
  children?: ExecutionStats[];
}

/**
 * JSON format output for EXPLAIN (PostgreSQL-compatible structure)
 * This provides a properly typed interface for formatJsonNode output.
 */
export interface JsonExplainNode {
  /** Node type (e.g., "Seq Scan", "Hash Join") */
  'Node Type': string;

  /** Relation/table name */
  'Relation Name'?: string;

  /** Alias */
  'Alias'?: string;

  /** Index name (for index scans) */
  'Index Name'?: string;

  /** Startup cost estimate */
  'Startup Cost'?: number;

  /** Total cost estimate */
  'Total Cost'?: number;

  /** Estimated row count */
  'Plan Rows'?: number;

  /** Estimated width in bytes */
  'Plan Width'?: number;

  /** Filter condition */
  'Filter'?: string;

  /** Index condition */
  'Index Cond'?: string;

  /** Hash join condition */
  'Hash Cond'?: string;

  /** Merge join condition */
  'Merge Cond'?: string;

  /** Sort keys (as array) */
  'Sort Key'?: string[];

  /** Group keys (as array) */
  'Group Key'?: string[];

  /** Output columns */
  'Output'?: string[];

  /** Actual rows (for ANALYZE) */
  'Actual Rows'?: number;

  /** Actual startup time (for ANALYZE) */
  'Actual Startup Time'?: number;

  /** Actual total time (for ANALYZE) */
  'Actual Total Time'?: number;

  /** Actual loops (for ANALYZE) */
  'Actual Loops'?: number;

  /** Child plans */
  Plans?: JsonExplainNode[];
}

/**
 * Supported SQL value types for formatValue
 */
export type FormatableValue = string | number | bigint | boolean | Date | Uint8Array | null;

// =============================================================================
// EXPLAIN GENERATOR
// =============================================================================

/**
 * Generate EXPLAIN output for a physical plan
 */
export function explain(
  plan: PhysicalPlanNode,
  options: ExplainOptions = {}
): string {
  const opts = { ...DEFAULT_EXPLAIN_OPTIONS, ...options };
  const explainNode = buildExplainNode(plan, opts);

  switch (opts.format) {
    case 'json':
      return formatJson(explainNode);
    case 'yaml':
      return formatYaml(explainNode, 0);
    case 'tree':
      return formatTree(explainNode, '', true);
    case 'text':
    default:
      return formatText(explainNode, 0, opts);
  }
}

/**
 * Generate EXPLAIN ANALYZE output
 */
export function explainAnalyze(
  plan: PhysicalPlanNode,
  stats: ExecutionStats,
  options: ExplainOptions = {}
): string {
  const opts = { ...DEFAULT_EXPLAIN_OPTIONS, ...options, analyze: true };
  const explainNode = buildExplainNode(plan, opts);
  const nodeWithStats = addExecutionStats(explainNode, stats);

  switch (opts.format) {
    case 'json':
      return formatJson(nodeWithStats);
    case 'yaml':
      return formatYaml(nodeWithStats, 0);
    case 'tree':
      return formatTree(nodeWithStats, '', true);
    case 'text':
    default:
      return formatText(nodeWithStats, 0, opts);
  }
}

// =============================================================================
// BUILD EXPLAIN NODE
// =============================================================================

/**
 * Build an explain node from a physical plan node
 */
function buildExplainNode(
  plan: PhysicalPlanNode,
  options: Required<ExplainOptions>
): ExplainNode {
  const node: ExplainNode = {
    nodeType: getNodeTypeName(plan),
    operation: getOperationDescription(plan),
    cost: options.costs ? {
      startup: plan.cost.startupCost,
      total: plan.cost.totalCost,
    } : undefined,
    rows: options.rowEstimates ? plan.cost.estimatedRows : undefined,
    memory: options.memory ? plan.cost.memoryBytes : undefined,
    output: options.verbose ? plan.outputColumns : undefined,
    children: plan.children.length > 0
      ? plan.children.map(c => buildExplainNode(c, options))
      : undefined,
  };

  // Add node-specific properties
  switch (plan.nodeType) {
    case 'scan':
      addScanProperties(node, plan as ScanNode);
      break;
    case 'filter':
      addFilterProperties(node, plan as FilterNode);
      break;
    case 'join':
      addJoinProperties(node, plan as JoinNode);
      break;
    case 'aggregate':
      addAggregateProperties(node, plan as AggregateNode);
      break;
    case 'sort':
      addSortProperties(node, plan as SortNode);
      break;
    case 'limit':
      addLimitProperties(node, plan as LimitNode);
      break;
  }

  return node;
}

/**
 * Get human-readable node type name
 */
function getNodeTypeName(plan: PhysicalPlanNode): string {
  switch (plan.nodeType) {
    case 'scan': {
      const scan = plan as ScanNode;
      switch (scan.accessMethod) {
        case 'seqScan':
          return 'Seq Scan';
        case 'indexScan':
          return 'Index Scan';
        case 'indexOnlyScan':
          return 'Index Only Scan';
        case 'bitmapIndexScan':
          return 'Bitmap Index Scan';
        case 'tidScan':
          return 'TID Scan';
        default:
          return 'Scan';
      }
    }
    case 'filter':
      return 'Filter';
    case 'join': {
      const join = plan as JoinNode;
      switch (join.algorithm) {
        case 'nestedLoop':
          return 'Nested Loop';
        case 'hash':
          return 'Hash Join';
        case 'merge':
          return 'Merge Join';
        case 'indexNested':
          return 'Index Nested Loop';
        default:
          return 'Join';
      }
    }
    case 'aggregate': {
      const agg = plan as AggregateNode;
      switch (agg.strategy) {
        case 'plain':
          return 'Aggregate';
        case 'sorted':
          return 'GroupAggregate';
        case 'hashed':
          return 'HashAggregate';
        default:
          return 'Aggregate';
      }
    }
    case 'sort': {
      const sort = plan as SortNode;
      switch (sort.method) {
        case 'quicksort':
          return 'Sort';
        case 'external':
          return 'External Sort';
        case 'top-n':
          return 'Top-N Heapsort';
        default:
          return 'Sort';
      }
    }
    case 'limit':
      return 'Limit';
    case 'distinct':
      return 'Unique';
    case 'union': {
      const unionPlan = plan as UnionNode;
      return unionPlan.all ? 'Append' : 'HashSetOp Union';
    }
    case 'merge':
      return 'Merge Append';
    case 'project':
      return 'Project';
    default:
      return plan.nodeType;
  }
}

/**
 * Get operation description
 */
function getOperationDescription(plan: PhysicalPlanNode): string {
  switch (plan.nodeType) {
    case 'scan': {
      const scan = plan as ScanNode;
      if (scan.accessMethod === 'indexScan' || scan.accessMethod === 'indexOnlyScan') {
        return `using ${scan.indexName} on ${scan.table}`;
      }
      return `on ${scan.table}`;
    }
    case 'join': {
      const join = plan as JoinNode;
      return `${join.joinType.toUpperCase()} JOIN`;
    }
    case 'aggregate': {
      const agg = plan as AggregateNode;
      if (agg.groupBy.length > 0) {
        const keys = agg.groupBy.map(formatExpression).join(', ');
        return `Key: ${keys}`;
      }
      return '';
    }
    case 'sort': {
      const sort = plan as SortNode;
      const keys = sort.sortKeys.map(k =>
        `${formatExpression(k.expr)} ${k.direction.toUpperCase()}`
      ).join(', ');
      return `Key: ${keys}`;
    }
    case 'limit': {
      const limit = plan as LimitNode;
      return limit.offset
        ? `Limit ${limit.limit} Offset ${limit.offset}`
        : `Limit ${limit.limit}`;
    }
    default:
      return '';
  }
}

/**
 * Add scan-specific properties
 */
function addScanProperties(node: ExplainNode, scan: ScanNode): void {
  node.relationName = scan.table;
  node.alias = scan.alias;
  node.accessMethod = scan.accessMethod;

  if (scan.indexName) {
    node.indexName = scan.indexName;
  }

  if (scan.indexCondition && scan.indexCondition.length > 0) {
    node.indexCondition = scan.indexCondition
      .map(c => `${c.column} ${formatOperator(c.operator)} ${formatValue(c.value)}`)
      .join(' AND ');
  }

  if (scan.filterCondition) {
    node.filter = formatPredicate(scan.filterCondition);
  }
}

/**
 * Add filter-specific properties
 */
function addFilterProperties(node: ExplainNode, filter: FilterNode): void {
  node.filter = formatPredicate(filter.predicate);
}

/**
 * Add join-specific properties
 */
function addJoinProperties(node: ExplainNode, join: JoinNode): void {
  if (join.condition) {
    const conditionStr = formatPredicate(join.condition);
    switch (join.algorithm) {
      case 'hash':
        node.hashCondition = conditionStr;
        break;
      case 'merge':
        node.mergeCondition = conditionStr;
        break;
      default:
        node.joinCondition = conditionStr;
    }
  }

  node.properties = {
    joinType: join.joinType,
    algorithm: join.algorithm,
  };

  if (join.buildSide) {
    node.properties.buildSide = join.buildSide;
  }
}

/**
 * Add aggregate-specific properties
 */
function addAggregateProperties(node: ExplainNode, agg: AggregateNode): void {
  if (agg.groupBy.length > 0) {
    node.groupKey = agg.groupBy.map(formatExpression).join(', ');
  }

  if (agg.having) {
    node.filter = formatPredicate(agg.having);
  }

  node.properties = {
    strategy: agg.strategy,
    numAggregates: agg.aggregates.length,
  };
}

/**
 * Add sort-specific properties
 */
function addSortProperties(node: ExplainNode, sort: SortNode): void {
  node.sortKey = sort.sortKeys
    .map(k => {
      let str = formatExpression(k.expr);
      str += ` ${k.direction.toUpperCase()}`;
      if (k.nullsFirst !== undefined) {
        str += k.nullsFirst ? ' NULLS FIRST' : ' NULLS LAST';
      }
      return str;
    })
    .join(', ');

  node.properties = {
    method: sort.method,
  };

  if (sort.limit) {
    node.properties.limit = sort.limit;
  }
}

/**
 * Add limit-specific properties
 */
function addLimitProperties(node: ExplainNode, limit: LimitNode): void {
  node.properties = {
    limit: limit.limit,
  };

  if (limit.offset) {
    node.properties.offset = limit.offset;
  }
}

// =============================================================================
// FORMATTING HELPERS
// =============================================================================

/**
 * Format a predicate as a string
 */
function formatPredicate(pred: Predicate): string {
  switch (pred.type) {
    case 'comparison':
      return `${formatExpression(pred.left)} ${formatOperator(pred.op)} ${formatExpression(pred.right)}`;

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
      return 'unknown';
  }
}

/**
 * Format an expression as a string
 */
function formatExpression(expr: Expression): string {
  switch (expr.type) {
    case 'columnRef':
      return expr.table ? `${expr.table}.${expr.column}` : expr.column;

    case 'literal':
      return formatValue(expr.value);

    case 'binary': {
      const op = formatOperator(expr.op);
      return `(${formatExpression(expr.left)} ${op} ${formatExpression(expr.right)})`;
    }

    case 'unary':
      if (expr.op === 'neg') return `-${formatExpression(expr.operand)}`;
      if (expr.op === 'not') return `NOT ${formatExpression(expr.operand)}`;
      return `${expr.op} ${formatExpression(expr.operand)}`;

    case 'function':
      return `${expr.name}(${expr.args.map(formatExpression).join(', ')})`;

    case 'aggregate': {
      const arg = expr.arg === '*' ? '*' : formatExpression(expr.arg);
      return `${expr.function.toUpperCase()}(${expr.distinct ? 'DISTINCT ' : ''}${arg})`;
    }

    case 'case': {
      let str = 'CASE';
      for (const w of expr.when) {
        str += ` WHEN ${formatExpression(w.condition)} THEN ${formatExpression(w.result)}`;
      }
      if (expr.else) {
        str += ` ELSE ${formatExpression(expr.else)}`;
      }
      return str + ' END';
    }

    case 'subquery':
      return '(subquery)';

    default:
      return 'unknown';
  }
}

/**
 * Format an operator
 */
function formatOperator(op: string): string {
  const operators: Record<string, string> = {
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
    add: '+',
    sub: '-',
    mul: '*',
    div: '/',
    mod: '%',
    and: 'AND',
    or: 'OR',
  };
  return operators[op] ?? op;
}

/**
 * Format a value for display
 */
function formatValue(value: FormatableValue): string {
  if (value === null) return 'NULL';
  if (typeof value === 'string') return `'${value}'`;
  if (typeof value === 'number') return String(value);
  if (typeof value === 'bigint') return String(value);
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  if (value instanceof Date) return `'${value.toISOString()}'`;
  if (value instanceof Uint8Array) return `'\\x${Buffer.from(value).toString('hex')}'`;
  // Exhaustive check - this line should never be reached
  const _exhaustive: never = value;
  return String(_exhaustive);
}

// =============================================================================
// OUTPUT FORMATTERS
// =============================================================================

/**
 * Format as text (PostgreSQL-style)
 */
function formatText(
  node: ExplainNode,
  depth: number,
  options: Required<ExplainOptions>
): string {
  const lines: string[] = [];
  const indent = ' '.repeat(depth * options.indent);
  const arrow = depth > 0 ? '-> ' : '';

  // Main node line
  let mainLine = `${indent}${arrow}${node.nodeType}`;
  if (node.operation) {
    mainLine += ` ${node.operation}`;
  }
  if (node.alias && node.alias !== node.relationName) {
    mainLine += ` ${node.alias}`;
  }

  lines.push(mainLine);

  // Cost and rows
  if (options.costs && node.cost) {
    const costLine = `${indent}   (cost=${node.cost.startup.toFixed(2)}..${node.cost.total.toFixed(2)}`;
    const rowsPart = options.rowEstimates && node.rows !== undefined ? ` rows=${node.rows}` : '';
    const memoryPart = options.memory && node.memory ? ` memory=${formatBytes(node.memory)}` : '';
    lines.push(`${costLine}${rowsPart}${memoryPart})`);
  }

  // Actual execution stats
  if (options.analyze && node.actualRows !== undefined) {
    let actualLine = `${indent}   (actual`;
    if (node.actualTime) {
      actualLine += ` time=${node.actualTime.startup.toFixed(3)}..${node.actualTime.total.toFixed(3)}`;
    }
    actualLine += ` rows=${node.actualRows}`;
    if (node.loops && node.loops > 1) {
      actualLine += ` loops=${node.loops}`;
    }
    actualLine += ')';
    lines.push(actualLine);
  }

  // Index condition
  if (node.indexCondition) {
    lines.push(`${indent}   Index Cond: (${node.indexCondition})`);
  }

  // Filter
  if (node.filter) {
    lines.push(`${indent}   Filter: (${node.filter})`);
  }

  // Join conditions
  if (node.hashCondition) {
    lines.push(`${indent}   Hash Cond: (${node.hashCondition})`);
  }
  if (node.mergeCondition) {
    lines.push(`${indent}   Merge Cond: (${node.mergeCondition})`);
  }
  if (node.joinCondition) {
    lines.push(`${indent}   Join Cond: (${node.joinCondition})`);
  }

  // Sort key
  if (node.sortKey) {
    lines.push(`${indent}   Sort Key: ${node.sortKey}`);
  }

  // Group key
  if (node.groupKey) {
    lines.push(`${indent}   Group Key: ${node.groupKey}`);
  }

  // Output (verbose mode)
  if (options.verbose && node.output && node.output.length > 0) {
    lines.push(`${indent}   Output: ${node.output.join(', ')}`);
  }

  // Children
  if (node.children) {
    for (const child of node.children) {
      lines.push(formatText(child, depth + 1, options));
    }
  }

  return lines.join('\n');
}

/**
 * Format as JSON
 */
function formatJson(node: ExplainNode): string {
  return JSON.stringify([{ Plan: formatJsonNode(node) }], null, 2);
}

/**
 * Format a node for JSON output
 */
function formatJsonNode(node: ExplainNode): JsonExplainNode {
  const result: JsonExplainNode = {
    'Node Type': node.nodeType,
  };

  if (node.relationName) result['Relation Name'] = node.relationName;
  if (node.alias) result['Alias'] = node.alias;
  if (node.indexName) result['Index Name'] = node.indexName;
  if (node.cost) {
    result['Startup Cost'] = node.cost.startup;
    result['Total Cost'] = node.cost.total;
  }
  if (node.rows !== undefined) result['Plan Rows'] = node.rows;
  if (node.width !== undefined) result['Plan Width'] = node.width;
  if (node.filter) result['Filter'] = node.filter;
  if (node.indexCondition) result['Index Cond'] = node.indexCondition;
  if (node.hashCondition) result['Hash Cond'] = node.hashCondition;
  if (node.mergeCondition) result['Merge Cond'] = node.mergeCondition;
  if (node.sortKey) result['Sort Key'] = [node.sortKey];
  if (node.groupKey) result['Group Key'] = [node.groupKey];
  if (node.output) result['Output'] = node.output;
  if (node.actualRows !== undefined) result['Actual Rows'] = node.actualRows;
  if (node.actualTime) {
    result['Actual Startup Time'] = node.actualTime.startup;
    result['Actual Total Time'] = node.actualTime.total;
  }
  if (node.loops !== undefined) result['Actual Loops'] = node.loops;

  if (node.children && node.children.length > 0) {
    result['Plans'] = node.children.map(formatJsonNode);
  }

  return result;
}

/**
 * Format as YAML
 */
function formatYaml(node: ExplainNode, depth: number): string {
  const indent = '  '.repeat(depth);
  const lines: string[] = [];

  lines.push(`${indent}- Node Type: "${node.nodeType}"`);

  if (node.relationName) {
    lines.push(`${indent}  Relation Name: "${node.relationName}"`);
  }
  if (node.alias) {
    lines.push(`${indent}  Alias: "${node.alias}"`);
  }
  if (node.indexName) {
    lines.push(`${indent}  Index Name: "${node.indexName}"`);
  }
  if (node.cost) {
    lines.push(`${indent}  Startup Cost: ${node.cost.startup}`);
    lines.push(`${indent}  Total Cost: ${node.cost.total}`);
  }
  if (node.rows !== undefined) {
    lines.push(`${indent}  Plan Rows: ${node.rows}`);
  }
  if (node.filter) {
    lines.push(`${indent}  Filter: "${node.filter}"`);
  }
  if (node.indexCondition) {
    lines.push(`${indent}  Index Cond: "${node.indexCondition}"`);
  }
  if (node.sortKey) {
    lines.push(`${indent}  Sort Key:`);
    lines.push(`${indent}    - "${node.sortKey}"`);
  }
  if (node.output && node.output.length > 0) {
    lines.push(`${indent}  Output:`);
    for (const col of node.output) {
      lines.push(`${indent}    - "${col}"`);
    }
  }

  if (node.children && node.children.length > 0) {
    lines.push(`${indent}  Plans:`);
    for (const child of node.children) {
      lines.push(formatYaml(child, depth + 2));
    }
  }

  return lines.join('\n');
}

/**
 * Format as ASCII tree
 */
function formatTree(node: ExplainNode, prefix: string, isLast: boolean): string {
  const lines: string[] = [];
  const connector = isLast ? '`-- ' : '|-- ';
  const childPrefix = prefix + (isLast ? '    ' : '|   ');

  let line = `${prefix}${connector}${node.nodeType}`;
  if (node.relationName) {
    line += ` on ${node.relationName}`;
  }
  if (node.indexName) {
    line += ` using ${node.indexName}`;
  }
  lines.push(line);

  if (node.children) {
    for (let i = 0; i < node.children.length; i++) {
      const isChildLast = i === node.children.length - 1;
      lines.push(formatTree(node.children[i], childPrefix, isChildLast));
    }
  }

  return lines.join('\n');
}

/**
 * Format bytes for display
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)}MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)}GB`;
}

// =============================================================================
// ANALYZE HELPERS
// =============================================================================

/**
 * Add execution statistics to an explain node
 */
function addExecutionStats(
  node: ExplainNode,
  stats: ExecutionStats
): ExplainNode {
  return {
    ...node,
    actualRows: stats.actualRows,
    actualTime: {
      startup: stats.startupTimeMs,
      total: stats.totalTimeMs,
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
// EXPLAIN GENERATOR CLASS
// =============================================================================

/**
 * Explain generator class for repeated use
 */
export class ExplainGenerator {
  private readonly options: Required<ExplainOptions>;

  constructor(options: ExplainOptions = {}) {
    this.options = { ...DEFAULT_EXPLAIN_OPTIONS, ...options };
  }

  /**
   * Generate EXPLAIN output
   */
  explain(plan: PhysicalPlanNode): string {
    return explain(plan, this.options);
  }

  /**
   * Generate EXPLAIN ANALYZE output
   */
  explainAnalyze(plan: PhysicalPlanNode, stats: ExecutionStats): string {
    return explainAnalyze(plan, stats, this.options);
  }

  /**
   * Build an explain node (for custom formatting)
   */
  buildExplainNode(plan: PhysicalPlanNode): ExplainNode {
    return buildExplainNode(plan, this.options);
  }
}

/**
 * Create an explain generator
 */
export function createExplainGenerator(options?: ExplainOptions): ExplainGenerator {
  return new ExplainGenerator(options);
}

// =============================================================================
// QUERY PLAN SUMMARY
// =============================================================================

/**
 * Generate a brief summary of a query plan
 */
export function summarizePlan(plan: PhysicalPlanNode): string {
  const parts: string[] = [];

  function visit(node: PhysicalPlanNode): void {
    switch (node.nodeType) {
      case 'scan': {
        const scan = node as ScanNode;
        if (scan.accessMethod === 'seqScan') {
          parts.push(`SeqScan(${scan.table})`);
        } else {
          parts.push(`IdxScan(${scan.indexName} on ${scan.table})`);
        }
        break;
      }
      case 'join': {
        const join = node as JoinNode;
        parts.push(`${join.algorithm}Join`);
        break;
      }
      case 'aggregate': {
        const agg = node as AggregateNode;
        parts.push(`Agg(${agg.strategy})`);
        break;
      }
      case 'sort':
        parts.push('Sort');
        break;
      case 'limit':
        parts.push('Limit');
        break;
    }

    for (const child of node.children) {
      visit(child);
    }
  }

  visit(plan);
  return parts.join(' -> ');
}

/**
 * Get the estimated total cost of a plan
 */
export function getTotalCost(plan: PhysicalPlanNode): number {
  return plan.cost.totalCost;
}

/**
 * Get the estimated row count of a plan
 */
export function getEstimatedRows(plan: PhysicalPlanNode): number {
  return plan.cost.estimatedRows;
}
