/**
 * DoSQL Workload Router
 *
 * Defines clear boundaries between OLTP (B-tree) and OLAP (columnar) execution paths.
 * Classifies queries based on their characteristics and routes them to the appropriate
 * storage and execution strategy.
 *
 * ## Workload Types
 *
 * - **OLTP** (Online Transaction Processing): Point lookups, small range scans, writes.
 *   Uses B-tree storage for low-latency row-oriented access.
 *
 * - **OLAP** (Online Analytical Processing): Full table scans, aggregations, analytics.
 *   Uses columnar storage for efficient analytical queries.
 *
 * - **HYBRID**: Queries that benefit from both paths (e.g., aggregate with filter).
 *   May use merge execution to combine hot (B-tree) and cold (columnar) data.
 *
 * ## Routing Decision Flow
 *
 * ```
 * SQL Query → Workload Classifier → WorkloadType
 *                    ↓
 *            Routing Decision
 *            /      |      \
 *         OLTP   HYBRID   OLAP
 *          ↓       ↓       ↓
 *       B-tree   Merge  Columnar
 * ```
 *
 * @example
 * ```typescript
 * import { WorkloadClassifier, WorkloadType } from './workload-router.js';
 *
 * const classifier = new WorkloadClassifier(schema);
 *
 * // Point lookup - OLTP
 * classifier.classify('SELECT * FROM users WHERE id = 1'); // OLTP
 *
 * // Aggregation - OLAP
 * classifier.classify('SELECT COUNT(*) FROM orders GROUP BY region'); // OLAP
 *
 * // Recent data with aggregation - HYBRID
 * classifier.classify('SELECT SUM(amount) FROM orders WHERE created_at > NOW() - INTERVAL 1 DAY'); // HYBRID
 * ```
 *
 * @packageDocumentation
 */

import type { QueryPlan, Schema, Predicate, Expression, ScanPlan } from './types.js';
import type { ParsedSelect, ParsedExpr, ParsedColumn } from '../parser/subquery.js';
import { SubqueryParser } from '../parser/subquery.js';

// =============================================================================
// WORKLOAD TYPE
// =============================================================================

/**
 * Workload type classification for query routing.
 *
 * Determines which execution path to use:
 * - OLTP: B-tree based row storage for transactional workloads
 * - OLAP: Columnar storage for analytical workloads
 * - HYBRID: Combined execution using both storage paths
 */
export enum WorkloadType {
  /**
   * Online Transaction Processing
   *
   * Characteristics:
   * - Point lookups by primary key
   * - Small range scans (< 1000 rows typically)
   * - INSERT/UPDATE/DELETE operations
   * - Real-time data access
   *
   * Execution: B-tree storage with index lookups
   */
  OLTP = 'oltp',

  /**
   * Online Analytical Processing
   *
   * Characteristics:
   * - Full table scans
   * - Aggregations (COUNT, SUM, AVG, etc.)
   * - GROUP BY operations
   * - Historical data analysis
   *
   * Execution: Columnar storage with projection/predicate pushdown
   */
  OLAP = 'olap',

  /**
   * Hybrid workload requiring both paths
   *
   * Characteristics:
   * - Aggregation over recent/hot data
   * - Range queries spanning hot and cold tiers
   * - Real-time analytics
   *
   * Execution: Merge operator combining B-tree and columnar results
   */
  HYBRID = 'hybrid',
}

// =============================================================================
// ROUTING DECISION
// =============================================================================

/**
 * Routing decision with explanation
 */
export interface RoutingDecision {
  /** The classified workload type */
  workloadType: WorkloadType;

  /** Primary data source to use */
  primarySource: 'btree' | 'columnar' | 'both';

  /** Whether to use index lookup optimization */
  useIndexLookup: boolean;

  /** Whether to enable projection pushdown */
  projectionPushdown: boolean;

  /** Whether to enable predicate pushdown */
  predicatePushdown: boolean;

  /** Human-readable explanation of the routing decision */
  reason: string;

  /** Confidence score (0-1) for the classification */
  confidence: number;

  /** Signals that influenced the decision */
  signals: ClassificationSignal[];
}

/**
 * Signal that influenced workload classification
 */
export interface ClassificationSignal {
  /** Signal name */
  name: string;
  /** Signal value */
  value: string | number | boolean;
  /** Weight towards OLTP (negative) or OLAP (positive) */
  weight: number;
}

// =============================================================================
// CLASSIFICATION CONFIG
// =============================================================================

/**
 * Configuration for workload classification
 */
export interface ClassifierConfig {
  /** Row count threshold for preferring columnar (default: 1000) */
  columnarThreshold: number;

  /** LIMIT threshold for considering OLTP path (default: 100) */
  oltpLimitThreshold: number;

  /** Maximum selectivity for index lookup (0-1, default: 0.01) */
  indexLookupSelectivity: number;

  /** Whether to prefer hot data for recent queries (default: true) */
  preferHotForRecent: boolean;

  /** Recency threshold in milliseconds (default: 1 hour) */
  recencyThreshold: number;

  /** Minimum aggregates to strongly prefer OLAP (default: 1) */
  olapAggregateThreshold: number;
}

/**
 * Default classifier configuration
 */
export const DEFAULT_CLASSIFIER_CONFIG: Readonly<ClassifierConfig> = Object.freeze({
  columnarThreshold: 1000,
  oltpLimitThreshold: 100,
  indexLookupSelectivity: 0.01,
  preferHotForRecent: true,
  recencyThreshold: 60 * 60 * 1000, // 1 hour
  olapAggregateThreshold: 1,
});

// =============================================================================
// WORKLOAD CLASSIFIER
// =============================================================================

/**
 * Classifies SQL queries into workload types for routing.
 *
 * Uses heuristics based on query characteristics:
 * - Aggregates and GROUP BY → OLAP
 * - Point lookups on primary key → OLTP
 * - Small LIMIT clauses → OLTP
 * - Full table scans → OLAP
 * - Time-bounded queries on recent data → HYBRID
 */
export class WorkloadClassifier {
  private schema: Schema;
  private config: ClassifierConfig;
  private parser: SubqueryParser;

  constructor(schema: Schema, config?: Partial<ClassifierConfig>) {
    this.schema = schema;
    this.config = { ...DEFAULT_CLASSIFIER_CONFIG, ...config };
    this.parser = new SubqueryParser();
  }

  /**
   * Classify a SQL query string
   *
   * @param sql - The SQL query to classify
   * @returns Routing decision with workload type and execution hints
   */
  classify(sql: string): RoutingDecision {
    const parsed = this.parser.parse(sql);
    return this.classifyParsed(parsed);
  }

  /**
   * Classify a parsed SELECT statement
   *
   * @param parsed - The parsed SELECT statement
   * @returns Routing decision with workload type and execution hints
   */
  classifyParsed(parsed: ParsedSelect): RoutingDecision {
    const signals: ClassificationSignal[] = [];
    let olapScore = 0;
    let oltpScore = 0;

    // Signal: Has aggregates
    const aggregates = this.countAggregates(parsed.columns);
    if (aggregates > 0) {
      const weight = aggregates >= this.config.olapAggregateThreshold ? 3 : 2;
      signals.push({ name: 'aggregates', value: aggregates, weight });
      olapScore += weight;
    }

    // Signal: Has GROUP BY
    if (parsed.groupBy && parsed.groupBy.length > 0) {
      signals.push({ name: 'groupBy', value: parsed.groupBy.length, weight: 2 });
      olapScore += 2;
    }

    // Signal: Has HAVING
    if (parsed.having) {
      signals.push({ name: 'having', value: true, weight: 1 });
      olapScore += 1;
    }

    // Signal: Primary key lookup
    if (this.isPrimaryKeyLookup(parsed)) {
      signals.push({ name: 'primaryKeyLookup', value: true, weight: -4 });
      oltpScore += 4;
    }

    // Signal: Small LIMIT
    if (parsed.limit !== undefined && parsed.limit <= this.config.oltpLimitThreshold) {
      signals.push({ name: 'smallLimit', value: parsed.limit, weight: -2 });
      oltpScore += 2;
    }

    // Signal: Large or no LIMIT (full scan)
    if (parsed.limit === undefined) {
      signals.push({ name: 'noLimit', value: true, weight: 1 });
      olapScore += 1;
    } else if (parsed.limit > this.config.columnarThreshold) {
      signals.push({ name: 'largeLimit', value: parsed.limit, weight: 1 });
      olapScore += 1;
    }

    // Signal: Joins
    if (parsed.joins && parsed.joins.length > 0) {
      // Joins are complex - depends on join type and cardinality
      // For now, treat as HYBRID candidate
      signals.push({ name: 'joins', value: parsed.joins.length, weight: 0 });
    }

    // Signal: ORDER BY without LIMIT (expensive full sort)
    if (parsed.orderBy && parsed.orderBy.length > 0 && parsed.limit === undefined) {
      signals.push({ name: 'fullSort', value: true, weight: 1 });
      olapScore += 1;
    }

    // Signal: DISTINCT
    if (parsed.distinct) {
      signals.push({ name: 'distinct', value: true, weight: 1 });
      olapScore += 1;
    }

    // Signal: Window functions (checked in columns)
    const hasWindowFunctions = this.hasWindowFunctions(parsed.columns);
    if (hasWindowFunctions) {
      signals.push({ name: 'windowFunctions', value: true, weight: 2 });
      olapScore += 2;
    }

    // Signal: Recency filter (WHERE created_at > NOW() - INTERVAL)
    const hasRecencyFilter = this.hasRecencyFilter(parsed.where);
    if (hasRecencyFilter && this.config.preferHotForRecent) {
      signals.push({ name: 'recencyFilter', value: true, weight: -1 });
      oltpScore += 1;
    }

    // Calculate final scores and decision
    const totalScore = olapScore - oltpScore;
    const maxScore = Math.max(olapScore, oltpScore, 1);
    const confidence = Math.abs(totalScore) / (maxScore + 3); // Normalize to 0-1 range

    let workloadType: WorkloadType;
    let primarySource: 'btree' | 'columnar' | 'both';
    let reason: string;

    if (oltpScore >= olapScore + 2) {
      // Strong OLTP signal
      workloadType = WorkloadType.OLTP;
      primarySource = 'btree';
      reason = this.buildReason('OLTP', signals.filter(s => s.weight < 0));
    } else if (olapScore >= oltpScore + 2) {
      // Strong OLAP signal
      workloadType = WorkloadType.OLAP;
      primarySource = 'columnar';
      reason = this.buildReason('OLAP', signals.filter(s => s.weight > 0));
    } else {
      // Mixed signals - use HYBRID
      workloadType = WorkloadType.HYBRID;
      primarySource = 'both';
      reason = this.buildReason('HYBRID', signals);
    }

    // Determine optimizations based on workload
    const useIndexLookup = workloadType === WorkloadType.OLTP && this.isPrimaryKeyLookup(parsed);
    const projectionPushdown = workloadType !== WorkloadType.OLTP;
    const predicatePushdown = true; // Always beneficial

    return {
      workloadType,
      primarySource,
      useIndexLookup,
      projectionPushdown,
      predicatePushdown,
      reason,
      confidence: Math.min(confidence, 1),
      signals,
    };
  }

  /**
   * Classify a query plan (post-planning)
   *
   * @param plan - The query plan to classify
   * @returns Routing decision
   */
  classifyPlan(plan: QueryPlan): RoutingDecision {
    const signals: ClassificationSignal[] = [];
    let olapScore = 0;
    let oltpScore = 0;

    // Walk the plan tree
    this.walkPlan(plan, (node) => {
      switch (node.type) {
        case 'scan':
          if (node.source === 'columnar') {
            signals.push({ name: 'columnarScan', value: true, weight: 2 });
            olapScore += 2;
          } else if (node.source === 'btree') {
            signals.push({ name: 'btreeScan', value: true, weight: -1 });
            oltpScore += 1;
          }
          break;

        case 'indexLookup':
          signals.push({ name: 'indexLookup', value: node.index, weight: -3 });
          oltpScore += 3;
          break;

        case 'aggregate':
          signals.push({ name: 'aggregate', value: node.aggregates.length, weight: 2 });
          olapScore += 2;
          break;

        case 'sort':
          if (node.estimatedRows && node.estimatedRows > this.config.columnarThreshold) {
            signals.push({ name: 'largSort', value: node.estimatedRows, weight: 1 });
            olapScore += 1;
          }
          break;

        case 'limit':
          if (node.limit <= this.config.oltpLimitThreshold) {
            signals.push({ name: 'smallLimit', value: node.limit, weight: -2 });
            oltpScore += 2;
          }
          break;

        case 'merge':
          signals.push({ name: 'merge', value: true, weight: 0 });
          break;
      }
    });

    // Calculate decision
    const totalScore = olapScore - oltpScore;
    const maxScore = Math.max(olapScore, oltpScore, 1);
    const confidence = Math.abs(totalScore) / (maxScore + 3);

    let workloadType: WorkloadType;
    let primarySource: 'btree' | 'columnar' | 'both';

    if (oltpScore >= olapScore + 2) {
      workloadType = WorkloadType.OLTP;
      primarySource = 'btree';
    } else if (olapScore >= oltpScore + 2) {
      workloadType = WorkloadType.OLAP;
      primarySource = 'columnar';
    } else {
      workloadType = WorkloadType.HYBRID;
      primarySource = 'both';
    }

    return {
      workloadType,
      primarySource,
      useIndexLookup: signals.some(s => s.name === 'indexLookup'),
      projectionPushdown: workloadType !== WorkloadType.OLTP,
      predicatePushdown: true,
      reason: this.buildReason(workloadType, signals),
      confidence: Math.min(confidence, 1),
      signals,
    };
  }

  // ===========================================================================
  // HELPER METHODS
  // ===========================================================================

  private countAggregates(columns: ParsedColumn[]): number {
    let count = 0;
    for (const col of columns) {
      if (col.expr.type === 'aggregate') {
        count++;
      }
    }
    return count;
  }

  private hasWindowFunctions(columns: ParsedColumn[]): boolean {
    for (const col of columns) {
      if (this.exprHasWindow(col.expr)) {
        return true;
      }
    }
    return false;
  }

  private exprHasWindow(expr: ParsedExpr): boolean {
    if (expr.type === 'function' && 'over' in expr) {
      return true;
    }
    // Check nested expressions
    if (expr.type === 'binary') {
      return this.exprHasWindow(expr.left) || this.exprHasWindow(expr.right);
    }
    if (expr.type === 'unary') {
      return this.exprHasWindow(expr.operand);
    }
    return false;
  }

  private isPrimaryKeyLookup(parsed: ParsedSelect): boolean {
    if (!parsed.where || !parsed.from) return false;

    const tableName = parsed.from.type === 'table' ? parsed.from.table : parsed.from.alias;
    const tableSchema = this.schema.tables.get(tableName);
    if (!tableSchema?.primaryKey) return false;

    // Check if WHERE is equality on primary key
    return this.isEqualityOnColumn(parsed.where, tableSchema.primaryKey);
  }

  private isEqualityOnColumn(expr: ParsedExpr, columns: string[]): boolean {
    if (expr.type === 'binary' && expr.op === 'eq') {
      const left = expr.left;
      if (left.type === 'column' && columns.includes(left.name)) {
        return true;
      }
    }
    // Check AND conditions
    if (expr.type === 'binary' && expr.op === 'and') {
      return this.isEqualityOnColumn(expr.left, columns) ||
             this.isEqualityOnColumn(expr.right, columns);
    }
    return false;
  }

  private hasRecencyFilter(where: ParsedExpr | undefined): boolean {
    if (!where) return false;

    // Look for patterns like: created_at > NOW() - INTERVAL or timestamp > ?
    const checkRecency = (expr: ParsedExpr): boolean => {
      if (expr.type === 'binary') {
        // Check for comparison with time-related column
        if (['gt', 'ge', 'lt', 'le'].includes(expr.op)) {
          const left = expr.left;
          if (left.type === 'column') {
            const name = left.name.toLowerCase();
            if (name.includes('time') || name.includes('date') || name.includes('created') || name.includes('updated')) {
              return true;
            }
          }
        }
        // Recurse into AND/OR
        if (expr.op === 'and' || expr.op === 'or') {
          return checkRecency(expr.left) || checkRecency(expr.right);
        }
      }
      return false;
    };

    return checkRecency(where);
  }

  private walkPlan(plan: QueryPlan, visitor: (node: QueryPlan) => void): void {
    visitor(plan);

    switch (plan.type) {
      case 'filter':
      case 'project':
      case 'aggregate':
      case 'sort':
      case 'limit':
      case 'distinct':
        this.walkPlan(plan.input, visitor);
        break;
      case 'join':
        this.walkPlan(plan.left, visitor);
        this.walkPlan(plan.right, visitor);
        break;
      case 'union':
      case 'merge':
        for (const input of plan.inputs) {
          this.walkPlan(input, visitor);
        }
        break;
      // scan and indexLookup are leaf nodes
    }
  }

  private buildReason(type: WorkloadType | string, signals: ClassificationSignal[]): string {
    const signalNames = signals
      .filter(s => s.weight !== 0)
      .map(s => s.name)
      .slice(0, 3);

    if (signalNames.length === 0) {
      return `Classified as ${type} based on query structure`;
    }

    return `Classified as ${type} due to: ${signalNames.join(', ')}`;
  }
}

// =============================================================================
// EXECUTION PATH INTERFACE
// =============================================================================

/**
 * Execution path interface for OLTP and OLAP workloads.
 *
 * Both paths implement this interface to provide a unified execution model
 * while allowing different underlying implementations.
 */
export interface ExecutionPath {
  /** Path identifier */
  readonly pathType: WorkloadType;

  /** Execute the query plan and return results */
  execute<T>(plan: QueryPlan): Promise<ExecutionResult<T>>;

  /** Check if this path can execute the given plan */
  canExecute(plan: QueryPlan): boolean;

  /** Estimated cost for executing the plan on this path */
  estimateCost(plan: QueryPlan): PathCost;
}

/**
 * Execution result from a path
 */
export interface ExecutionResult<T extends Record<string, unknown>> {
  /** Result rows */
  rows: T[];

  /** Execution statistics */
  stats: PathExecutionStats;
}

/**
 * Cost estimate for a path
 */
export interface PathCost {
  /** Estimated I/O operations */
  ioOps: number;

  /** Estimated CPU cost */
  cpuCost: number;

  /** Estimated memory usage (bytes) */
  memoryBytes: number;

  /** Estimated total time (ms) */
  estimatedTimeMs: number;
}

/**
 * Execution statistics from a path
 */
export interface PathExecutionStats {
  /** Actual execution time (ms) */
  executionTimeMs: number;

  /** Rows scanned */
  rowsScanned: number;

  /** Rows returned */
  rowsReturned: number;

  /** Bytes read */
  bytesRead: number;

  /** Storage path used */
  storagePath: 'btree' | 'columnar' | 'both';

  /** Cache hits (if applicable) */
  cacheHits?: number;

  /** Index lookups (for OLTP) */
  indexLookups?: number;

  /** Row groups scanned (for OLAP) */
  rowGroupsScanned?: number;

  /** Row groups skipped via zone maps (for OLAP) */
  rowGroupsSkipped?: number;
}

// =============================================================================
// WORKLOAD ROUTER
// =============================================================================

/**
 * Routes queries to the appropriate execution path based on workload classification.
 *
 * The router:
 * 1. Classifies the query workload type
 * 2. Selects the optimal execution path
 * 3. Optionally merges results from multiple paths for HYBRID queries
 */
export class WorkloadRouter {
  private classifier: WorkloadClassifier;
  private oltpPath: ExecutionPath | null = null;
  private olapPath: ExecutionPath | null = null;

  constructor(schema: Schema, config?: Partial<ClassifierConfig>) {
    this.classifier = new WorkloadClassifier(schema, config);
  }

  /**
   * Register an OLTP execution path (B-tree based)
   */
  registerOLTPPath(path: ExecutionPath): void {
    if (path.pathType !== WorkloadType.OLTP) {
      throw new Error(`Expected OLTP path, got ${path.pathType}`);
    }
    this.oltpPath = path;
  }

  /**
   * Register an OLAP execution path (columnar based)
   */
  registerOLAPPath(path: ExecutionPath): void {
    if (path.pathType !== WorkloadType.OLAP) {
      throw new Error(`Expected OLAP path, got ${path.pathType}`);
    }
    this.olapPath = path;
  }

  /**
   * Route a query to the appropriate execution path
   *
   * @param sql - SQL query string
   * @returns Routing decision
   */
  route(sql: string): RoutingDecision {
    return this.classifier.classify(sql);
  }

  /**
   * Route a parsed query to the appropriate execution path
   *
   * @param parsed - Parsed SELECT statement
   * @returns Routing decision
   */
  routeParsed(parsed: ParsedSelect): RoutingDecision {
    return this.classifier.classifyParsed(parsed);
  }

  /**
   * Route a query plan to the appropriate execution path
   *
   * @param plan - Query plan
   * @returns Routing decision
   */
  routePlan(plan: QueryPlan): RoutingDecision {
    return this.classifier.classifyPlan(plan);
  }

  /**
   * Get the execution path for a workload type
   *
   * @param workloadType - The workload type
   * @returns The registered execution path, or null if not registered
   */
  getPath(workloadType: WorkloadType): ExecutionPath | null {
    switch (workloadType) {
      case WorkloadType.OLTP:
        return this.oltpPath;
      case WorkloadType.OLAP:
        return this.olapPath;
      case WorkloadType.HYBRID:
        // For HYBRID, caller should use both paths
        return null;
    }
  }

  /**
   * Check if the router has the required paths for a workload type
   */
  hasPath(workloadType: WorkloadType): boolean {
    switch (workloadType) {
      case WorkloadType.OLTP:
        return this.oltpPath !== null;
      case WorkloadType.OLAP:
        return this.olapPath !== null;
      case WorkloadType.HYBRID:
        return this.oltpPath !== null && this.olapPath !== null;
    }
  }
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Create a workload classifier with default configuration
 *
 * @param schema - Database schema
 * @returns WorkloadClassifier instance
 */
export function createWorkloadClassifier(schema: Schema): WorkloadClassifier {
  return new WorkloadClassifier(schema);
}

/**
 * Create a workload router with default configuration
 *
 * @param schema - Database schema
 * @returns WorkloadRouter instance
 */
export function createWorkloadRouter(schema: Schema): WorkloadRouter {
  return new WorkloadRouter(schema);
}

/**
 * Quick classification of a SQL query
 *
 * @param sql - SQL query string
 * @param schema - Database schema
 * @returns WorkloadType
 */
export function classifyQuery(sql: string, schema: Schema): WorkloadType {
  const classifier = new WorkloadClassifier(schema);
  return classifier.classify(sql).workloadType;
}

/**
 * Check if a query should use OLAP path
 *
 * @param sql - SQL query string
 * @param schema - Database schema
 * @returns true if OLAP or HYBRID
 */
export function shouldUseColumnar(sql: string, schema: Schema): boolean {
  const workloadType = classifyQuery(sql, schema);
  return workloadType === WorkloadType.OLAP || workloadType === WorkloadType.HYBRID;
}

/**
 * Check if a query should use OLTP path
 *
 * @param sql - SQL query string
 * @param schema - Database schema
 * @returns true if OLTP
 */
export function shouldUseBTree(sql: string, schema: Schema): boolean {
  const workloadType = classifyQuery(sql, schema);
  return workloadType === WorkloadType.OLTP;
}
