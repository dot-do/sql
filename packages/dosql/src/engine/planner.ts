/**
 * DoSQL Query Planner
 *
 * Transforms SQL queries into execution plans.
 * - Parses SQL into an AST
 * - Converts AST to logical plan
 * - Optimizes and converts to physical plan
 * - Decides between B-tree (OLTP) and Columnar (OLAP) execution paths
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

// =============================================================================
// SQL PARSING (Simplified AST)
// =============================================================================

/**
 * Parsed SELECT statement
 */
export interface ParsedSelect {
  type: 'select';
  columns: ParsedColumn[];
  from: ParsedFrom;
  joins?: ParsedJoin[];
  where?: ParsedExpr;
  groupBy?: ParsedExpr[];
  having?: ParsedExpr;
  orderBy?: ParsedOrderBy[];
  limit?: number;
  offset?: number;
  distinct?: boolean;
}

/**
 * Parsed column in SELECT
 */
export interface ParsedColumn {
  expr: ParsedExpr;
  alias?: string;
}

/**
 * Parsed FROM clause
 */
export interface ParsedFrom {
  table: string;
  alias?: string;
}

/**
 * Parsed JOIN clause
 */
export interface ParsedJoin {
  type: 'inner' | 'left' | 'right' | 'full' | 'cross';
  table: string;
  alias?: string;
  on?: ParsedExpr;
}

/**
 * Parsed ORDER BY item
 */
export interface ParsedOrderBy {
  expr: ParsedExpr;
  direction: 'asc' | 'desc';
  nullsFirst?: boolean;
}

/**
 * Parsed expression types
 */
export type ParsedExpr =
  | { type: 'column'; name: string; table?: string }
  | { type: 'literal'; value: string | number | boolean | null }
  | { type: 'binary'; op: string; left: ParsedExpr; right: ParsedExpr }
  | { type: 'unary'; op: string; operand: ParsedExpr }
  | { type: 'function'; name: string; args: ParsedExpr[] }
  | { type: 'aggregate'; name: string; arg: ParsedExpr | '*'; distinct?: boolean }
  | { type: 'between'; expr: ParsedExpr; low: ParsedExpr; high: ParsedExpr }
  | { type: 'in'; expr: ParsedExpr; values: ParsedExpr[] }
  | { type: 'isNull'; expr: ParsedExpr; isNot: boolean }
  | { type: 'star' };

// =============================================================================
// SQL TOKENIZER
// =============================================================================

type TokenType =
  | 'keyword'
  | 'identifier'
  | 'number'
  | 'string'
  | 'operator'
  | 'punctuation'
  | 'parameter'
  | 'eof';

interface Token {
  type: TokenType;
  value: string;
  position: number;
}

const KEYWORDS = new Set([
  'select', 'from', 'where', 'and', 'or', 'not', 'in', 'between', 'like',
  'is', 'null', 'true', 'false', 'as', 'on', 'join', 'inner', 'left', 'right',
  'full', 'outer', 'cross', 'group', 'by', 'having', 'order', 'asc', 'desc',
  'limit', 'offset', 'distinct', 'all', 'union', 'intersect', 'except',
  'count', 'sum', 'avg', 'min', 'max', 'nulls', 'first', 'last',
]);

function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;

  while (pos < sql.length) {
    // Skip whitespace
    while (pos < sql.length && /\s/.test(sql[pos])) pos++;
    if (pos >= sql.length) break;

    const start = pos;
    const char = sql[pos];

    // String literal
    if (char === "'" || char === '"') {
      const quote = char;
      pos++;
      while (pos < sql.length && sql[pos] !== quote) {
        if (sql[pos] === '\\') pos++; // Escape
        pos++;
      }
      pos++; // closing quote
      tokens.push({ type: 'string', value: sql.slice(start + 1, pos - 1), position: start });
      continue;
    }

    // Number
    if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(sql[pos + 1] || ''))) {
      while (pos < sql.length && /[0-9.]/.test(sql[pos])) pos++;
      tokens.push({ type: 'number', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Parameter ($1, $2, etc.)
    if (char === '$') {
      pos++;
      while (pos < sql.length && /[0-9]/.test(sql[pos])) pos++;
      tokens.push({ type: 'parameter', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(char)) {
      while (pos < sql.length && /[a-zA-Z0-9_]/.test(sql[pos])) pos++;
      const value = sql.slice(start, pos);
      const lower = value.toLowerCase();
      if (KEYWORDS.has(lower)) {
        tokens.push({ type: 'keyword', value: lower, position: start });
      } else {
        tokens.push({ type: 'identifier', value, position: start });
      }
      continue;
    }

    // Operators (multi-char)
    if (sql.slice(pos, pos + 2) === '<=' || sql.slice(pos, pos + 2) === '>=' ||
        sql.slice(pos, pos + 2) === '<>' || sql.slice(pos, pos + 2) === '!=') {
      tokens.push({ type: 'operator', value: sql.slice(pos, pos + 2), position: start });
      pos += 2;
      continue;
    }

    // Single char operators and punctuation
    if ('=<>+-*/%'.includes(char)) {
      tokens.push({ type: 'operator', value: char, position: start });
      pos++;
      continue;
    }

    if ('(),;.'.includes(char)) {
      tokens.push({ type: 'punctuation', value: char, position: start });
      pos++;
      continue;
    }

    // Unknown - skip
    pos++;
  }

  tokens.push({ type: 'eof', value: '', position: pos });
  return tokens;
}

// =============================================================================
// SQL PARSER
// =============================================================================

class Parser {
  private tokens: Token[];
  private pos = 0;

  constructor(sql: string) {
    this.tokens = tokenize(sql);
  }

  private current(): Token {
    return this.tokens[this.pos] || { type: 'eof', value: '', position: -1 };
  }

  private peek(offset = 0): Token {
    return this.tokens[this.pos + offset] || { type: 'eof', value: '', position: -1 };
  }

  private advance(): Token {
    return this.tokens[this.pos++] || { type: 'eof', value: '', position: -1 };
  }

  private expect(type: TokenType, value?: string): Token {
    const token = this.current();
    if (token.type !== type || (value !== undefined && token.value.toLowerCase() !== value.toLowerCase())) {
      throw new Error(`Expected ${type}${value ? ` '${value}'` : ''}, got ${token.type} '${token.value}'`);
    }
    return this.advance();
  }

  private match(type: TokenType, value?: string): boolean {
    const token = this.current();
    return token.type === type && (value === undefined || token.value.toLowerCase() === value.toLowerCase());
  }

  private matchKeyword(...keywords: string[]): boolean {
    const token = this.current();
    return token.type === 'keyword' && keywords.includes(token.value.toLowerCase());
  }

  parse(): ParsedSelect {
    return this.parseSelect();
  }

  private parseSelect(): ParsedSelect {
    this.expect('keyword', 'select');

    // DISTINCT
    const distinct = this.matchKeyword('distinct');
    if (distinct) this.advance();

    // Columns
    const columns = this.parseSelectList();

    // FROM
    this.expect('keyword', 'from');
    const from = this.parseTableRef();

    // JOINs
    const joins: ParsedJoin[] = [];
    while (this.matchKeyword('join', 'inner', 'left', 'right', 'full', 'cross')) {
      joins.push(this.parseJoin());
    }

    // WHERE
    let where: ParsedExpr | undefined;
    if (this.matchKeyword('where')) {
      this.advance();
      where = this.parseExpression();
    }

    // GROUP BY
    let groupBy: ParsedExpr[] | undefined;
    if (this.matchKeyword('group')) {
      this.advance();
      this.expect('keyword', 'by');
      groupBy = [this.parseExpression()];
      while (this.match('punctuation', ',')) {
        this.advance();
        groupBy.push(this.parseExpression());
      }
    }

    // HAVING
    let having: ParsedExpr | undefined;
    if (this.matchKeyword('having')) {
      this.advance();
      having = this.parseExpression();
    }

    // ORDER BY
    let orderBy: ParsedOrderBy[] | undefined;
    if (this.matchKeyword('order')) {
      this.advance();
      this.expect('keyword', 'by');
      orderBy = [this.parseOrderByItem()];
      while (this.match('punctuation', ',')) {
        this.advance();
        orderBy.push(this.parseOrderByItem());
      }
    }

    // LIMIT
    let limit: number | undefined;
    if (this.matchKeyword('limit')) {
      this.advance();
      limit = parseInt(this.expect('number').value, 10);
    }

    // OFFSET
    let offset: number | undefined;
    if (this.matchKeyword('offset')) {
      this.advance();
      offset = parseInt(this.expect('number').value, 10);
    }

    return { type: 'select', columns, from, joins: joins.length > 0 ? joins : undefined, where, groupBy, having, orderBy, limit, offset, distinct };
  }

  private parseSelectList(): ParsedColumn[] {
    const columns: ParsedColumn[] = [];
    columns.push(this.parseSelectColumn());
    while (this.match('punctuation', ',')) {
      this.advance();
      columns.push(this.parseSelectColumn());
    }
    return columns;
  }

  private parseSelectColumn(): ParsedColumn {
    // Handle *
    if (this.match('operator', '*')) {
      this.advance();
      return { expr: { type: 'star' } };
    }

    const expr = this.parseExpression();

    // AS alias
    let alias: string | undefined;
    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier')) {
      // Implicit alias (no AS keyword)
      alias = this.advance().value;
    }

    return { expr, alias };
  }

  private parseTableRef(): ParsedFrom {
    const table = this.expect('identifier').value;
    let alias: string | undefined;
    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier') && !this.matchKeyword('join', 'inner', 'left', 'right', 'full', 'cross', 'where', 'group', 'having', 'order', 'limit', 'offset')) {
      alias = this.advance().value;
    }
    return { table, alias };
  }

  private parseJoin(): ParsedJoin {
    let type: ParsedJoin['type'] = 'inner';

    if (this.matchKeyword('left')) {
      type = 'left';
      this.advance();
      if (this.matchKeyword('outer')) this.advance();
    } else if (this.matchKeyword('right')) {
      type = 'right';
      this.advance();
      if (this.matchKeyword('outer')) this.advance();
    } else if (this.matchKeyword('full')) {
      type = 'full';
      this.advance();
      if (this.matchKeyword('outer')) this.advance();
    } else if (this.matchKeyword('cross')) {
      type = 'cross';
      this.advance();
    } else if (this.matchKeyword('inner')) {
      this.advance();
    }

    this.expect('keyword', 'join');
    const { table, alias } = this.parseTableRef();

    let on: ParsedExpr | undefined;
    if (this.matchKeyword('on')) {
      this.advance();
      on = this.parseExpression();
    }

    return { type, table, alias, on };
  }

  private parseOrderByItem(): ParsedOrderBy {
    const expr = this.parseExpression();
    let direction: 'asc' | 'desc' = 'asc';
    let nullsFirst: boolean | undefined;

    if (this.matchKeyword('asc')) {
      this.advance();
    } else if (this.matchKeyword('desc')) {
      direction = 'desc';
      this.advance();
    }

    if (this.matchKeyword('nulls')) {
      this.advance();
      if (this.matchKeyword('first')) {
        nullsFirst = true;
        this.advance();
      } else if (this.matchKeyword('last')) {
        nullsFirst = false;
        this.advance();
      }
    }

    return { expr, direction, nullsFirst };
  }

  private parseExpression(): ParsedExpr {
    return this.parseOr();
  }

  private parseOr(): ParsedExpr {
    let left = this.parseAnd();
    while (this.matchKeyword('or')) {
      this.advance();
      const right = this.parseAnd();
      left = { type: 'binary', op: 'or', left, right };
    }
    return left;
  }

  private parseAnd(): ParsedExpr {
    let left = this.parseNot();
    while (this.matchKeyword('and')) {
      this.advance();
      const right = this.parseNot();
      left = { type: 'binary', op: 'and', left, right };
    }
    return left;
  }

  private parseNot(): ParsedExpr {
    if (this.matchKeyword('not')) {
      this.advance();
      return { type: 'unary', op: 'not', operand: this.parseNot() };
    }
    return this.parseComparison();
  }

  private parseComparison(): ParsedExpr {
    let left = this.parseAddSub();

    // IS NULL / IS NOT NULL
    if (this.matchKeyword('is')) {
      this.advance();
      const isNot = this.matchKeyword('not');
      if (isNot) this.advance();
      this.expect('keyword', 'null');
      return { type: 'isNull', expr: left, isNot };
    }

    // BETWEEN
    if (this.matchKeyword('between')) {
      this.advance();
      const low = this.parseAddSub();
      this.expect('keyword', 'and');
      const high = this.parseAddSub();
      return { type: 'between', expr: left, low, high };
    }

    // IN
    if (this.matchKeyword('in')) {
      this.advance();
      this.expect('punctuation', '(');
      const values: ParsedExpr[] = [this.parseExpression()];
      while (this.match('punctuation', ',')) {
        this.advance();
        values.push(this.parseExpression());
      }
      this.expect('punctuation', ')');
      return { type: 'in', expr: left, values };
    }

    // NOT IN / NOT BETWEEN
    if (this.matchKeyword('not')) {
      this.advance();
      if (this.matchKeyword('in')) {
        this.advance();
        this.expect('punctuation', '(');
        const values: ParsedExpr[] = [this.parseExpression()];
        while (this.match('punctuation', ',')) {
          this.advance();
          values.push(this.parseExpression());
        }
        this.expect('punctuation', ')');
        return { type: 'unary', op: 'not', operand: { type: 'in', expr: left, values } };
      }
      if (this.matchKeyword('between')) {
        this.advance();
        const low = this.parseAddSub();
        this.expect('keyword', 'and');
        const high = this.parseAddSub();
        return { type: 'unary', op: 'not', operand: { type: 'between', expr: left, low, high } };
      }
    }

    // Comparison operators
    const opMap: Record<string, string> = {
      '=': 'eq', '<>': 'ne', '!=': 'ne', '<': 'lt', '<=': 'le', '>': 'gt', '>=': 'ge',
    };
    if (this.current().type === 'operator' && opMap[this.current().value]) {
      const op = opMap[this.advance().value];
      const right = this.parseAddSub();
      return { type: 'binary', op, left, right };
    }

    // LIKE
    if (this.matchKeyword('like')) {
      this.advance();
      const right = this.parseAddSub();
      return { type: 'binary', op: 'like', left, right };
    }

    return left;
  }

  private parseAddSub(): ParsedExpr {
    let left = this.parseMulDiv();
    while (this.match('operator', '+') || this.match('operator', '-')) {
      const op = this.advance().value === '+' ? 'add' : 'sub';
      const right = this.parseMulDiv();
      left = { type: 'binary', op, left, right };
    }
    return left;
  }

  private parseMulDiv(): ParsedExpr {
    let left = this.parseUnary();
    while (this.match('operator', '*') || this.match('operator', '/') || this.match('operator', '%')) {
      const opChar = this.advance().value;
      const op = opChar === '*' ? 'mul' : opChar === '/' ? 'div' : 'mod';
      const right = this.parseUnary();
      left = { type: 'binary', op, left, right };
    }
    return left;
  }

  private parseUnary(): ParsedExpr {
    if (this.match('operator', '-')) {
      this.advance();
      return { type: 'unary', op: 'neg', operand: this.parseUnary() };
    }
    return this.parsePrimary();
  }

  private parsePrimary(): ParsedExpr {
    const token = this.current();

    // Parenthesized expression
    if (this.match('punctuation', '(')) {
      this.advance();
      const expr = this.parseExpression();
      this.expect('punctuation', ')');
      return expr;
    }

    // Aggregate functions
    if (this.matchKeyword('count', 'sum', 'avg', 'min', 'max')) {
      const name = this.advance().value.toLowerCase();
      this.expect('punctuation', '(');

      let distinct = false;
      if (this.matchKeyword('distinct')) {
        distinct = true;
        this.advance();
      }

      let arg: ParsedExpr | '*';
      if (this.match('operator', '*')) {
        this.advance();
        arg = '*';
      } else {
        arg = this.parseExpression();
      }

      this.expect('punctuation', ')');
      return { type: 'aggregate', name, arg, distinct };
    }

    // Function call or column
    if (this.match('identifier')) {
      const name = this.advance().value;

      // Function call
      if (this.match('punctuation', '(')) {
        this.advance();
        const args: ParsedExpr[] = [];
        if (!this.match('punctuation', ')')) {
          args.push(this.parseExpression());
          while (this.match('punctuation', ',')) {
            this.advance();
            args.push(this.parseExpression());
          }
        }
        this.expect('punctuation', ')');
        return { type: 'function', name, args };
      }

      // Table.column
      if (this.match('punctuation', '.')) {
        this.advance();
        const column = this.expect('identifier').value;
        return { type: 'column', name: column, table: name };
      }

      return { type: 'column', name };
    }

    // Number literal
    if (this.match('number')) {
      const value = this.advance().value;
      return { type: 'literal', value: value.includes('.') ? parseFloat(value) : parseInt(value, 10) };
    }

    // String literal
    if (this.match('string')) {
      return { type: 'literal', value: this.advance().value };
    }

    // Boolean literals
    if (this.matchKeyword('true')) {
      this.advance();
      return { type: 'literal', value: true };
    }
    if (this.matchKeyword('false')) {
      this.advance();
      return { type: 'literal', value: false };
    }

    // NULL
    if (this.matchKeyword('null')) {
      this.advance();
      return { type: 'literal', value: null };
    }

    // Parameter
    if (this.match('parameter')) {
      const param = this.advance().value;
      return { type: 'column', name: param }; // Treat as column ref, resolve later
    }

    throw new Error(`Unexpected token: ${token.type} '${token.value}'`);
  }
}

/**
 * Parse a SQL query string into an AST
 */
export function parseSQL(sql: string): ParsedSelect {
  return new Parser(sql).parse();
}

// =============================================================================
// PLAN BUILDER
// =============================================================================

/**
 * Convert parsed expression to plan expression
 */
function toExpression(parsed: ParsedExpr): Expression {
  switch (parsed.type) {
    case 'column':
      return { type: 'columnRef', column: parsed.name, table: parsed.table };
    case 'literal':
      return lit(parsed.value);
    case 'star':
      return { type: 'columnRef', column: '*' };
    case 'binary': {
      const opMap: Record<string, ComparisonOp | 'add' | 'sub' | 'mul' | 'div' | 'mod' | 'and' | 'or'> = {
        eq: 'eq', ne: 'ne', lt: 'lt', le: 'le', gt: 'gt', ge: 'ge', like: 'like',
        add: 'add', sub: 'sub', mul: 'mul', div: 'div', mod: 'mod',
        and: 'and', or: 'or',
      };
      return {
        type: 'binary',
        op: opMap[parsed.op] || (parsed.op as any),
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
      return {
        type: 'aggregate',
        function: fnMap[parsed.name] || 'count',
        arg: parsed.arg === '*' ? '*' : toExpression(parsed.arg),
        distinct: parsed.distinct,
      };
    }
    case 'between':
    case 'in':
    case 'isNull':
      // These are converted to predicates, not expressions
      throw new Error(`${parsed.type} should be converted to predicate`);
    default:
      throw new Error(`Unknown expression type: ${(parsed as any).type}`);
  }
}

/**
 * Convert parsed expression to predicate
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
      const compOps = ['eq', 'ne', 'lt', 'le', 'gt', 'ge', 'like'];
      if (compOps.includes(parsed.op)) {
        return {
          type: 'comparison',
          op: parsed.op as ComparisonOp,
          left: toExpression(parsed.left),
          right: toExpression(parsed.right),
        };
      }
      // Arithmetic comparison
      throw new Error(`Cannot convert arithmetic to predicate: ${parsed.op}`);
    }
    case 'unary':
      if (parsed.op === 'not') {
        return {
          type: 'logical',
          op: 'not',
          operands: [toPredicate(parsed.operand)],
        };
      }
      throw new Error(`Unsupported unary predicate: ${parsed.op}`);
    case 'between':
      return {
        type: 'between',
        expr: toExpression(parsed.expr),
        low: toExpression(parsed.low),
        high: toExpression(parsed.high),
      };
    case 'in':
      return {
        type: 'in',
        expr: toExpression(parsed.expr),
        values: parsed.values.map(toExpression),
      };
    case 'isNull':
      return {
        type: 'isNull',
        expr: toExpression(parsed.expr),
        isNot: parsed.isNot,
      };
    default:
      throw new Error(`Cannot convert to predicate: ${parsed.type}`);
  }
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

    // Add projection
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

    // Add sort (ORDER BY)
    if (parsed.orderBy && parsed.orderBy.length > 0) {
      plan = this.buildSort(plan, parsed.orderBy);
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
    const source = this.decideDataSource(parsed);
    const tableSchema = this.schema.tables.get(parsed.from.table);
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

    const scan: ScanPlan = {
      id: nextPlanId(),
      type: 'scan',
      table: parsed.from.table,
      alias: parsed.from.alias,
      source,
      columns,
      predicate,
    };

    return scan;
  }

  /**
   * Try to use index lookup
   */
  private tryIndexLookup(parsed: ParsedSelect): IndexLookupPlan | null {
    if (!parsed.where) return null;

    const tableSchema = this.schema.tables.get(parsed.from.table);
    if (!tableSchema?.primaryKey) return null;

    // Check if WHERE is a simple equality on primary key
    if (parsed.where.type === 'binary' && parsed.where.op === 'eq') {
      const left = parsed.where.left;
      const right = parsed.where.right;

      if (left.type === 'column' && tableSchema.primaryKey.includes(left.name)) {
        return {
          id: nextPlanId(),
          type: 'indexLookup',
          table: parsed.from.table,
          alias: parsed.from.alias,
          index: 'primary',
          lookupKey: [toExpression(right)],
          columns: tableSchema.columns.map(c => c.name),
        };
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
      const rightScan: ScanPlan = {
        id: nextPlanId(),
        type: 'scan',
        table: join.table,
        alias: join.alias,
        source: 'btree', // Default to B-tree for joins
        columns: this.getTableColumns(join.table),
      };

      const joinPlan: JoinPlan = {
        id: nextPlanId(),
        type: 'join',
        joinType: join.type as JoinType,
        left: result,
        right: rightScan,
        condition: join.on ? toPredicate(join.on) : undefined,
        algorithm: 'hash', // Default to hash join
      };

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
        aggregates.push({
          expr: {
            type: 'aggregate',
            function: fnMap[col.expr.name] || 'count',
            arg: col.expr.arg === '*' ? '*' : toExpression(col.expr.arg),
            distinct: col.expr.distinct,
          },
          alias: col.alias || col.expr.name,
        });
      }
    }

    return {
      id: nextPlanId(),
      type: 'aggregate',
      input: plan,
      groupBy,
      aggregates,
      having: parsed.having ? toPredicate(parsed.having) : undefined,
    };
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
    return {
      id: nextPlanId(),
      type: 'sort',
      input: plan,
      orderBy: orderBy.map(o => ({
        expr: toExpression(o.expr),
        direction: o.direction,
        nullsFirst: o.nullsFirst,
      })),
    };
  }

  /**
   * Build limit/offset
   */
  private buildLimit(plan: QueryPlan, limit?: number, offset?: number): QueryPlan {
    return {
      id: nextPlanId(),
      type: 'limit',
      input: plan,
      limit: limit ?? Infinity,
      offset,
    };
  }

  // =============================================================================
  // HELPER METHODS
  // =============================================================================

  private hasAggregates(columns: ParsedColumn[]): boolean {
    return columns.some(c => c.expr.type === 'aggregate');
  }

  private isSelectStar(columns: ParsedColumn[]): boolean {
    return columns.length === 1 && columns[0].expr.type === 'star';
  }

  private isPrimaryKeyLookup(parsed: ParsedSelect): boolean {
    if (!parsed.where) return false;
    const tableSchema = this.schema.tables.get(parsed.from.table);
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
      result = `${pad}Unknown`;
  }

  return result;
}
