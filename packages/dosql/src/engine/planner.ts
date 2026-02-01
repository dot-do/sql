/**
 * DoSQL Query Planner
 *
 * Transforms SQL queries into execution plans.
 * - Uses unified parser to parse SQL into an AST
 * - Converts AST to logical plan
 * - Optimizes and converts to physical plan
 * - Decides between B-tree (OLTP) and Columnar (OLAP) execution paths
 *
 * Note: Parser code was consolidated into parser/subquery.ts per issue sql-0zgg.
 * The planner now uses SubqueryParser for parsing SQL into AST.
 */

import {
  type QueryPlan,
  type ScanPlan,
  type IndexLookupPlan,
  type FilterPlan,
  type ProjectPlan,
  type JoinPlan,
  type AggregatePlan,
  type SortPlan,
  type LimitPlan,
  type MergePlan,
  type DataSource,
  type Predicate,
  type Expression,
  type ColumnRef,
  type Literal,
  type BinaryExpr,
  type AggregateExpr,
  type SortSpec,
  type Schema,
  type ComparisonOp,
  type AggregateFunction,
  type JoinType,
  nextPlanId,
  col,
  lit,
} from './types.js';
import { assertNever } from '../utils/assert-never.js';
import { PlannerError } from '../errors/index.js';
import { PlannerErrorCode } from '../errors/codes.js';

// Import unified parser types from parser/subquery.ts
import {
  SubqueryParser,
  type ParsedSelect,
  type ParsedColumn,
  type ParsedFrom,
  type ParsedJoin,
  type ParsedOrderBy,
  type ParsedExpr,
  type SubqueryNode,
} from '../parser/subquery.js';

// =============================================================================
// PARSER ADAPTER
// =============================================================================

/**
 * Parse a SQL query string into an AST using the unified parser
 *
 * This function wraps SubqueryParser to provide a simple interface for parsing
 * SELECT statements. The unified parser in parser/subquery.ts supports:
 * - CTEs (WITH clause)
 * - Subqueries (scalar, IN, EXISTS, derived tables)
 * - CASE expressions
 * - Set operations (UNION/INTERSECT/EXCEPT)
 * - Full source location tracking
 *
 * @param sql - The SQL SELECT statement to parse
 * @returns The parsed SELECT statement AST
 */
export function parseSQL(sql: string): ParsedSelect {
  const parser = new SubqueryParser();
  return parser.parse(sql);
}

// Re-export types for backward compatibility
export type { ParsedSelect, ParsedColumn, ParsedFrom, ParsedJoin, ParsedOrderBy, ParsedExpr };

// =============================================================================
// PLAN BUILDER
// =============================================================================

/**
 * Convert parsed expression to plan expression
 *
 * Handles all expression types from the unified parser including:
 * - Basic types: column, literal, star
 * - Binary/unary expressions
 * - Functions and aggregates
 * - CASE expressions (simple and searched)
 * - Subqueries (scalar, EXISTS)
 */
function toExpression(parsed: ParsedExpr): Expression {
  switch (parsed.type) {
    case 'column': {
      // Handle optional table property with exactOptionalPropertyTypes
      const colRef: ColumnRef = { type: 'columnRef', column: parsed.name };
      if (parsed.table) {
        colRef.table = parsed.table;
      }
      return colRef;
    }
    case 'literal':
      return lit(parsed.value);
    case 'star':
      return { type: 'columnRef', column: '*' };
    case 'binary': {
      const opMap: Record<string, ComparisonOp | 'add' | 'sub' | 'mul' | 'div' | 'mod' | 'and' | 'or'> = {
        eq: 'eq', ne: 'ne', lt: 'lt', le: 'le', gt: 'gt', ge: 'ge', like: 'like', ilike: 'like',
        add: 'add', sub: 'sub', mul: 'mul', div: 'div', mod: 'mod',
        and: 'and', or: 'or',
      };
      return {
        type: 'binary',
        op: opMap[parsed.op] || (parsed.op as BinaryExpr['op']),
        left: toExpression(parsed.left),
        right: toExpression(parsed.right),
      };
    }
    case 'unary':
      return {
        type: 'unary',
        op: parsed.op as 'not' | 'neg' | 'isNull' | 'isNotNull',
        operand: toExpression(parsed.operand),
      };
    case 'function':
      return {
        type: 'function',
        name: parsed.name,
        args: parsed.args.map(toExpression),
      };
    case 'aggregate': {
      const fnMap: Record<string, AggregateFunction> = {
        count: 'count', sum: 'sum', avg: 'avg', min: 'min', max: 'max',
      };
      const aggExpr: AggregateExpr = {
        type: 'aggregate',
        function: fnMap[parsed.name] || 'count',
        arg: parsed.arg === '*' ? '*' : toExpression(parsed.arg),
      };
      if (parsed.distinct) {
        aggExpr.distinct = parsed.distinct;
      }
      return aggExpr;
    }
    case 'case': {
      // Convert CASE expression to CaseExpr
      const whenClauses: Array<{ condition: Expression; result: Expression } | { value: Expression; result: Expression }> = [];
      for (const when of parsed.whens) {
        whenClauses.push({
          condition: toExpression(when.condition),
          result: toExpression(when.result),
        });
      }
      const caseExpr: import('./types.js').CaseExpr = {
        type: 'case',
        when: whenClauses,
      };
      if (parsed.operand) {
        caseExpr.operand = toExpression(parsed.operand);
      }
      if (parsed.else_) {
        caseExpr.else = toExpression(parsed.else_);
      }
      return caseExpr;
    }
    case 'subquery':
    case 'exists':
      // Subqueries require plan building - for now, throw an error
      // Full subquery support would require recursive planning
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Subquery expressions require recursive planning`);
    case 'comparison':
      // Quantified comparison (ANY/ALL/SOME) - not yet supported
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Quantified comparisons (ANY/ALL/SOME) not yet supported`);
    case 'tuple':
      // Tuple expressions (for row value expressions) - not yet supported in plan expressions
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Tuple expressions not yet supported in plans`);
    case 'between':
    case 'in':
    case 'isNull':
      // These are converted to predicates, not expressions
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `${parsed.type} should be converted to predicate`);
    default: {
      // Handle any other types gracefully
      const exprType = (parsed as { type: string }).type;
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Unknown expression type: ${exprType}`);
    }
  }
}

/**
 * Convert parsed expression to predicate
 *
 * Handles predicate-style expressions from the unified parser.
 * Note: IN with subquery values requires special handling.
 */
function toPredicate(parsed: ParsedExpr): Predicate {
  switch (parsed.type) {
    case 'binary': {
      if (parsed.op === 'and' || parsed.op === 'or') {
        return {
          type: 'logical',
          op: parsed.op,
          operands: [toPredicate(parsed.left), toPredicate(parsed.right)],
        };
      }
      const compOps = ['eq', 'ne', 'lt', 'le', 'gt', 'ge', 'like', 'ilike'];
      if (compOps.includes(parsed.op)) {
        return {
          type: 'comparison',
          op: (parsed.op === 'ilike' ? 'like' : parsed.op) as ComparisonOp,
          left: toExpression(parsed.left),
          right: toExpression(parsed.right),
        };
      }
      // Arithmetic comparison
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Cannot convert arithmetic to predicate: ${parsed.op}`);
    }
    case 'unary':
      if (parsed.op === 'not') {
        return {
          type: 'logical',
          op: 'not',
          operands: [toPredicate(parsed.operand)],
        };
      }
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Unsupported unary predicate: ${parsed.op}`);
    case 'between':
      return {
        type: 'between',
        expr: toExpression(parsed.expr),
        low: toExpression(parsed.low),
        high: toExpression(parsed.high),
      };
    case 'in': {
      // IN clause can have either a list of values or a subquery
      // Check if values is an array (value list) or SubqueryNode
      if (Array.isArray(parsed.values)) {
        return {
          type: 'in',
          expr: toExpression(parsed.expr),
          values: parsed.values.map(toExpression),
        };
      } else {
        // IN with subquery - not yet fully supported
        throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `IN with subquery requires recursive planning`);
      }
    }
    case 'isNull':
      return {
        type: 'isNull',
        expr: toExpression(parsed.expr),
        isNot: parsed.isNot,
      };
    case 'exists':
      // EXISTS requires subquery planning
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `EXISTS predicate requires recursive planning`);
    case 'comparison':
      // Quantified comparison (ANY/ALL/SOME)
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Quantified comparisons (ANY/ALL/SOME) not yet supported`);
    default:
      throw new PlannerError(PlannerErrorCode.INVALID_PLAN, `Cannot convert to predicate: ${parsed.type}`);
  }
}

// =============================================================================
// HELPER FUNCTIONS FOR PARSED FROM/JOIN TYPES
// =============================================================================

/**
 * Extract table name from ParsedFrom (handles both table and derived table types)
 */
function getTableName(from: ParsedFrom): string {
  if (from.type === 'table') {
    return from.table;
  }
  // Derived table - use alias as the "table name"
  return from.alias;
}

/**
 * Extract alias from ParsedFrom
 */
function getTableAlias(from: ParsedFrom): string | undefined {
  if (from.type === 'table') {
    return from.alias;
  }
  return from.alias;
}

/**
 * Extract table name from ParsedJoin's table (which is a ParsedFrom)
 */
function getJoinTableName(join: ParsedJoin): string {
  return getTableName(join.table);
}

/**
 * Extract alias from ParsedJoin
 */
function getJoinAlias(join: ParsedJoin): string | undefined {
  // Use explicit alias if provided, otherwise derive from table
  if (join.alias) {
    return join.alias;
  }
  return getTableAlias(join.table);
}

/**
 * Planner options
 */
export interface PlannerOptions {
  /** Prefer B-tree for small result sets */
  preferBTree?: boolean;
  /** Row count threshold for using columnar */
  columnarThreshold?: number;
  /** Enable predicate pushdown */
  predicatePushdown?: boolean;
  /** Enable projection pushdown */
  projectionPushdown?: boolean;
}

const DEFAULT_PLANNER_OPTIONS: Required<PlannerOptions> = {
  preferBTree: true,
  columnarThreshold: 1000,
  predicatePushdown: true,
  projectionPushdown: true,
};

/**
 * Query planner class
 */
export class QueryPlanner {
  private schema: Schema;
  private options: Required<PlannerOptions>;

  constructor(schema: Schema, options?: PlannerOptions) {
    this.schema = schema;
    this.options = { ...DEFAULT_PLANNER_OPTIONS, ...options };
  }

  /**
   * Plan a SQL query
   */
  plan(sql: string): QueryPlan {
    const parsed = parseSQL(sql);
    return this.planSelect(parsed);
  }

  /**
   * Plan a parsed SELECT statement
   */
  private planSelect(parsed: ParsedSelect): QueryPlan {
    // Build base scan
    let plan = this.buildScan(parsed);

    // Add JOINs
    if (parsed.joins && parsed.joins.length > 0) {
      plan = this.buildJoins(plan, parsed);
    }

    // Add filter (WHERE)
    if (parsed.where) {
      plan = this.buildFilter(plan, parsed.where);
    }

    // Add aggregate (GROUP BY)
    if (parsed.groupBy || this.hasAggregates(parsed.columns)) {
      plan = this.buildAggregate(plan, parsed);
    }

    // Add HAVING filter
    if (parsed.having) {
      plan = this.buildFilter(plan, parsed.having);
    }

    // Add sort (ORDER BY) - BEFORE projection so sort can access non-selected columns
    if (parsed.orderBy && parsed.orderBy.length > 0) {
      plan = this.buildSort(plan, parsed.orderBy);
    }

    // Add projection - AFTER sort so ORDER BY can reference columns not in SELECT
    if (!this.isSelectStar(parsed.columns)) {
      plan = this.buildProject(plan, parsed);
    }

    // Add DISTINCT
    if (parsed.distinct) {
      plan = {
        id: nextPlanId(),
        type: 'distinct',
        input: plan,
      };
    }

    // Add limit/offset
    if (parsed.limit !== undefined || parsed.offset !== undefined) {
      plan = this.buildLimit(plan, parsed.limit, parsed.offset);
    }

    return plan;
  }

  /**
   * Decide data source based on query characteristics
   */
  private decideDataSource(parsed: ParsedSelect): DataSource {
    // Has aggregates -> prefer columnar
    if (this.hasAggregates(parsed.columns)) {
      return 'columnar';
    }

    // Point lookup by primary key -> B-tree
    if (this.isPrimaryKeyLookup(parsed)) {
      return 'btree';
    }

    // Small limit -> B-tree
    if (parsed.limit !== undefined && parsed.limit <= this.options.columnarThreshold) {
      return 'btree';
    }

    // Full scan or large range -> columnar
    return 'columnar';
  }

  /**
   * Build scan plan
   */
  private buildScan(parsed: ParsedSelect): QueryPlan {
    // Handle optional FROM clause (e.g., SELECT 1)
    if (!parsed.from) {
      // Return a simple scan with no table (values-only query)
      return {
        id: nextPlanId(),
        type: 'scan',
        table: '',
        source: 'btree',
        columns: [],
      };
    }

    const source = this.decideDataSource(parsed);
    const tableName = getTableName(parsed.from);
    const tableAlias = getTableAlias(parsed.from);
    const tableSchema = this.schema.tables.get(tableName);
    const columns = tableSchema
      ? tableSchema.columns.map(c => c.name)
      : ['*'];

    // Check for index lookup opportunity
    if (this.isPrimaryKeyLookup(parsed) && parsed.where) {
      const indexPlan = this.tryIndexLookup(parsed);
      if (indexPlan) return indexPlan;
    }

    // Pushdown predicate to scan if possible
    let predicate: Predicate | undefined;
    if (this.options.predicatePushdown && parsed.where && this.canPushdownPredicate(parsed.where)) {
      predicate = toPredicate(parsed.where);
    }

    // Build scan with exactOptionalPropertyTypes compliance
    const scan: ScanPlan = {
      id: nextPlanId(),
      type: 'scan',
      table: tableName,
      source,
      columns,
    };
    if (tableAlias) {
      scan.alias = tableAlias;
    }
    if (predicate) {
      scan.predicate = predicate;
    }

    return scan;
  }

  /**
   * Try to use index lookup
   */
  private tryIndexLookup(parsed: ParsedSelect): IndexLookupPlan | null {
    if (!parsed.where || !parsed.from) return null;

    const tableName = getTableName(parsed.from);
    const tableAlias = getTableAlias(parsed.from);
    const tableSchema = this.schema.tables.get(tableName);
    if (!tableSchema?.primaryKey) return null;

    // Check if WHERE is a simple equality on primary key
    if (parsed.where.type === 'binary' && parsed.where.op === 'eq') {
      const left = parsed.where.left;
      const right = parsed.where.right;

      if (left.type === 'column' && tableSchema.primaryKey.includes(left.name)) {
        const indexPlan: IndexLookupPlan = {
          id: nextPlanId(),
          type: 'indexLookup',
          table: tableName,
          index: 'primary',
          lookupKey: [toExpression(right)],
          columns: tableSchema.columns.map(c => c.name),
        };
        if (tableAlias) {
          indexPlan.alias = tableAlias;
        }
        return indexPlan;
      }
    }

    return null;
  }

  /**
   * Build JOINs
   */
  private buildJoins(plan: QueryPlan, parsed: ParsedSelect): QueryPlan {
    let result = plan;

    for (const join of parsed.joins!) {
      const joinTableName = getJoinTableName(join);
      const joinAlias = getJoinAlias(join);

      const rightScan: ScanPlan = {
        id: nextPlanId(),
        type: 'scan',
        table: joinTableName,
        source: 'btree', // Default to B-tree for joins
        columns: this.getTableColumns(joinTableName),
      };
      if (joinAlias) {
        rightScan.alias = joinAlias;
      }

      const joinPlan: JoinPlan = {
        id: nextPlanId(),
        type: 'join',
        joinType: join.type as JoinType,
        left: result,
        right: rightScan,
        algorithm: 'hash', // Default to hash join
      };
      if (join.on) {
        joinPlan.condition = toPredicate(join.on);
      }

      result = joinPlan;
    }

    return result;
  }

  /**
   * Build filter (WHERE / HAVING)
   */
  private buildFilter(plan: QueryPlan, where: ParsedExpr): QueryPlan {
    // If predicate was already pushed down to scan, skip
    if (plan.type === 'scan' && plan.predicate) {
      return plan;
    }

    return {
      id: nextPlanId(),
      type: 'filter',
      input: plan,
      predicate: toPredicate(where),
    };
  }

  /**
   * Build aggregate (GROUP BY)
   */
  private buildAggregate(plan: QueryPlan, parsed: ParsedSelect): QueryPlan {
    const groupBy = parsed.groupBy?.map(toExpression) || [];
    const aggregates: { expr: AggregateExpr; alias: string }[] = [];

    for (const col of parsed.columns) {
      if (col.expr.type === 'aggregate') {
        const fnMap: Record<string, AggregateFunction> = {
          count: 'count', sum: 'sum', avg: 'avg', min: 'min', max: 'max',
        };
        const aggExpr: AggregateExpr = {
          type: 'aggregate',
          function: fnMap[col.expr.name] || 'count',
          arg: col.expr.arg === '*' ? '*' : toExpression(col.expr.arg),
        };
        if (col.expr.distinct) {
          aggExpr.distinct = col.expr.distinct;
        }
        aggregates.push({
          expr: aggExpr,
          alias: col.alias || col.expr.name,
        });
      }
    }

    const aggPlan: AggregatePlan = {
      id: nextPlanId(),
      type: 'aggregate',
      input: plan,
      groupBy,
      aggregates,
    };
    if (parsed.having) {
      aggPlan.having = toPredicate(parsed.having);
    }
    return aggPlan;
  }

  /**
   * Build projection
   */
  private buildProject(plan: QueryPlan, parsed: ParsedSelect): QueryPlan {
    const expressions = parsed.columns
      .filter(c => c.expr.type !== 'star')
      .map(c => {
        const alias = c.alias || this.deriveAlias(c.expr);
        return { expr: toExpression(c.expr), alias };
      });

    return {
      id: nextPlanId(),
      type: 'project',
      input: plan,
      expressions,
    };
  }

  /**
   * Build sort (ORDER BY)
   */
  private buildSort(plan: QueryPlan, orderBy: ParsedOrderBy[]): QueryPlan {
    const sortSpecs: SortSpec[] = orderBy.map(o => {
      const spec: SortSpec = {
        expr: toExpression(o.expr),
        direction: o.direction,
      };
      if (o.nullsFirst !== undefined) {
        spec.nullsFirst = o.nullsFirst;
      }
      return spec;
    });

    return {
      id: nextPlanId(),
      type: 'sort',
      input: plan,
      orderBy: sortSpecs,
    };
  }

  /**
   * Build limit/offset
   */
  private buildLimit(plan: QueryPlan, limit?: number, offset?: number): QueryPlan {
    const limitPlan: LimitPlan = {
      id: nextPlanId(),
      type: 'limit',
      input: plan,
      limit: limit ?? Infinity,
    };
    if (offset !== undefined) {
      limitPlan.offset = offset;
    }
    return limitPlan;
  }

  // =============================================================================
  // HELPER METHODS
  // =============================================================================

  private hasAggregates(columns: ParsedColumn[]): boolean {
    return columns.some(c => c.expr.type === 'aggregate');
  }

  private isSelectStar(columns: ParsedColumn[]): boolean {
    const firstCol = columns[0];
    return columns.length === 1 && firstCol !== undefined && firstCol.expr.type === 'star';
  }

  private isPrimaryKeyLookup(parsed: ParsedSelect): boolean {
    if (!parsed.where || !parsed.from) return false;
    const tableName = getTableName(parsed.from);
    const tableSchema = this.schema.tables.get(tableName);
    if (!tableSchema?.primaryKey) return false;

    // Simple check: is WHERE an equality on primary key?
    if (parsed.where.type === 'binary' && parsed.where.op === 'eq') {
      const left = parsed.where.left;
      if (left.type === 'column' && tableSchema.primaryKey.includes(left.name)) {
        return true;
      }
    }

    return false;
  }

  private canPushdownPredicate(expr: ParsedExpr): boolean {
    // Simple predicates can be pushed down
    if (expr.type === 'binary') {
      const simpleOps = ['eq', 'ne', 'lt', 'le', 'gt', 'ge', 'and', 'or'];
      if (simpleOps.includes(expr.op)) {
        return this.canPushdownPredicate(expr.left) && this.canPushdownPredicate(expr.right);
      }
    }
    if (expr.type === 'column' || expr.type === 'literal') {
      return true;
    }
    if (expr.type === 'between' || expr.type === 'in' || expr.type === 'isNull') {
      return true;
    }
    return false;
  }

  private getTableColumns(table: string): string[] {
    const schema = this.schema.tables.get(table);
    return schema ? schema.columns.map(c => c.name) : ['*'];
  }

  private deriveAlias(expr: ParsedExpr): string {
    if (expr.type === 'column') return expr.name;
    if (expr.type === 'aggregate') return expr.name;
    return 'expr';
  }
}

/**
 * Create a query planner
 */
export function createPlanner(schema: Schema, options?: PlannerOptions): QueryPlanner {
  return new QueryPlanner(schema, options);
}

// =============================================================================
// PLAN UTILITIES
// =============================================================================

/**
 * Format a query plan as a string (for debugging)
 */
export function formatPlan(plan: QueryPlan, indent = 0): string {
  const pad = '  '.repeat(indent);
  let result = '';

  switch (plan.type) {
    case 'scan':
      result = `${pad}Scan ${plan.table}${plan.alias ? ` AS ${plan.alias}` : ''} [${plan.source}]`;
      if (plan.predicate) result += ` (filtered)`;
      break;
    case 'indexLookup':
      result = `${pad}IndexLookup ${plan.table}.${plan.index}`;
      break;
    case 'filter':
      result = `${pad}Filter\n${formatPlan(plan.input, indent + 1)}`;
      break;
    case 'project':
      result = `${pad}Project [${plan.expressions.map(e => e.alias).join(', ')}]\n${formatPlan(plan.input, indent + 1)}`;
      break;
    case 'join':
      result = `${pad}${plan.joinType.toUpperCase()} Join (${plan.algorithm})\n${formatPlan(plan.left, indent + 1)}\n${formatPlan(plan.right, indent + 1)}`;
      break;
    case 'aggregate':
      result = `${pad}Aggregate [${plan.aggregates.map(a => a.alias).join(', ')}]\n${formatPlan(plan.input, indent + 1)}`;
      break;
    case 'sort':
      result = `${pad}Sort [${plan.orderBy.map(o => o.direction).join(', ')}]\n${formatPlan(plan.input, indent + 1)}`;
      break;
    case 'limit':
      result = `${pad}Limit ${plan.limit}${plan.offset ? ` OFFSET ${plan.offset}` : ''}\n${formatPlan(plan.input, indent + 1)}`;
      break;
    case 'distinct':
      result = `${pad}Distinct\n${formatPlan(plan.input, indent + 1)}`;
      break;
    case 'union':
      result = `${pad}Union${plan.all ? ' All' : ''}\n${plan.inputs.map(i => formatPlan(i, indent + 1)).join('\n')}`;
      break;
    case 'merge':
      result = `${pad}Merge\n${plan.inputs.map(i => formatPlan(i, indent + 1)).join('\n')}`;
      break;
    default:
      return assertNever(plan, `Unknown plan type: ${(plan as unknown as { type: string }).type}`);
  }

  return result;
}
