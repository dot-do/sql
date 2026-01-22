/**
 * DoSQL Subquery Parser
 *
 * Extends the base SQL parser to support:
 * - Scalar subqueries in SELECT, WHERE, HAVING
 * - IN/NOT IN subqueries
 * - EXISTS/NOT EXISTS subqueries
 * - Correlated subqueries with outer references
 * - Derived tables (subqueries in FROM clause)
 * - ANY/ALL/SOME operators
 */

// =============================================================================
// AST TYPES
// =============================================================================

/**
 * Location information for error reporting
 */
export interface SourceLocation {
  line: number;
  column: number;
  offset: number;
}

/**
 * Correlated column reference (reference to outer query)
 */
export interface CorrelatedColumn {
  table?: string;
  column: string;
}

/**
 * Subquery node in the AST
 */
export interface SubqueryNode {
  type: 'subquery';
  subqueryType: 'scalar' | 'in' | 'exists' | 'derived' | 'any' | 'all';
  query: ParsedSelect;
  alias?: string;
  /** Columns referenced from outer query (for correlated subqueries) */
  correlatedColumns?: CorrelatedColumn[];
  /** Whether this subquery references outer query columns */
  isCorrelated: boolean;
  /** Optimization hints */
  canBeSemiJoin?: boolean;
  canBeAntiJoin?: boolean;
  location?: SourceLocation;
}

/**
 * EXISTS expression
 */
export interface ExistsExpression {
  type: 'exists';
  query: ParsedSelect;
  subqueryType: 'exists';
  correlatedColumns?: CorrelatedColumn[];
  isCorrelated: boolean;
  canBeSemiJoin?: boolean;
  location?: SourceLocation;
}

/**
 * Comparison with quantifier (ANY/ALL/SOME)
 */
export interface QuantifiedComparison {
  type: 'comparison';
  op: ComparisonOp;
  quantifier: 'any' | 'all' | 'some';
  left: ParsedExpr;
  right: SubqueryNode;
  location?: SourceLocation;
}

/**
 * Tuple expression for multi-column IN
 */
export interface TupleExpression {
  type: 'tuple';
  elements: ParsedExpr[];
  location?: SourceLocation;
}

/**
 * Derived table (subquery in FROM clause)
 */
export interface DerivedTable {
  type: 'derived';
  query: ParsedSelect;
  alias: string;
  columnAliases?: string[];
  lateral?: boolean;
  location?: SourceLocation;
}

/**
 * CTE (Common Table Expression) definition
 */
export interface CTEDefinition {
  name: string;
  columns?: string[];
  query: ParsedSelect;
  recursive?: boolean;
  location?: SourceLocation;
}

// =============================================================================
// EXTENDED PARSED TYPES
// =============================================================================

/**
 * Comparison operators
 */
export type ComparisonOp = 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge' | 'like' | 'ilike';

/**
 * Extended parsed expression types (including subqueries)
 */
export type ParsedExpr =
  | { type: 'column'; name: string; table?: string; location?: SourceLocation }
  | { type: 'literal'; value: string | number | boolean | null; location?: SourceLocation }
  | { type: 'binary'; op: string; left: ParsedExpr; right: ParsedExpr; location?: SourceLocation }
  | { type: 'unary'; op: string; operand: ParsedExpr; location?: SourceLocation }
  | { type: 'function'; name: string; args: ParsedExpr[]; location?: SourceLocation }
  | { type: 'aggregate'; name: string; arg: ParsedExpr | '*'; distinct?: boolean; location?: SourceLocation }
  | { type: 'between'; expr: ParsedExpr; low: ParsedExpr; high: ParsedExpr; location?: SourceLocation }
  | { type: 'in'; expr: ParsedExpr; values: ParsedExpr[] | SubqueryNode; not?: boolean; location?: SourceLocation }
  | { type: 'isNull'; expr: ParsedExpr; isNot: boolean; location?: SourceLocation }
  | { type: 'case'; operand?: ParsedExpr; whens: { condition: ParsedExpr; result: ParsedExpr }[]; else_?: ParsedExpr; location?: SourceLocation }
  | { type: 'star'; location?: SourceLocation }
  | SubqueryNode
  | ExistsExpression
  | QuantifiedComparison
  | TupleExpression;

/**
 * Extended parsed column
 */
export interface ParsedColumn {
  expr: ParsedExpr;
  alias?: string;
  location?: SourceLocation;
}

/**
 * Extended FROM clause (supports derived tables)
 */
export type ParsedFrom =
  | { type: 'table'; table: string; alias?: string; location?: SourceLocation }
  | DerivedTable;

/**
 * Extended JOIN clause
 */
export interface ParsedJoin {
  type: 'inner' | 'left' | 'right' | 'full' | 'cross';
  table: ParsedFrom;
  alias?: string;
  on?: ParsedExpr;
  lateral?: boolean;
  location?: SourceLocation;
}

/**
 * Extended ORDER BY item
 */
export interface ParsedOrderBy {
  expr: ParsedExpr;
  direction: 'asc' | 'desc';
  nullsFirst?: boolean;
  location?: SourceLocation;
}

/**
 * UNION/INTERSECT/EXCEPT clause
 */
export interface SetOperation {
  type: 'union' | 'intersect' | 'except';
  all?: boolean;
  query: ParsedSelect;
  location?: SourceLocation;
}

/**
 * Extended parsed SELECT statement
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
  cte?: CTEDefinition[];
  union?: SetOperation;
  location?: SourceLocation;
}

// =============================================================================
// TOKENIZER
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
  location: SourceLocation;
}

const KEYWORDS = new Set([
  'select', 'from', 'where', 'and', 'or', 'not', 'in', 'between', 'like', 'ilike',
  'is', 'null', 'true', 'false', 'as', 'on', 'join', 'inner', 'left', 'right',
  'full', 'outer', 'cross', 'natural', 'group', 'by', 'having', 'order', 'asc', 'desc',
  'limit', 'offset', 'distinct', 'all', 'union', 'intersect', 'except',
  'count', 'sum', 'avg', 'min', 'max', 'nulls', 'first', 'last',
  'exists', 'any', 'some', 'case', 'when', 'then', 'else', 'end',
  'with', 'recursive', 'lateral',
]);

class Tokenizer {
  private sql: string;
  private pos = 0;
  private line = 1;
  private column = 1;
  private tokens: Token[] = [];

  constructor(sql: string) {
    this.sql = sql;
  }

  tokenize(): Token[] {
    while (this.pos < this.sql.length) {
      this.skipWhitespaceAndComments();
      if (this.pos >= this.sql.length) break;
      this.readToken();
    }

    this.tokens.push({
      type: 'eof',
      value: '',
      location: { line: this.line, column: this.column, offset: this.pos },
    });

    return this.tokens;
  }

  private location(): SourceLocation {
    return { line: this.line, column: this.column, offset: this.pos };
  }

  private advance(): string {
    const char = this.sql[this.pos];
    this.pos++;
    if (char === '\n') {
      this.line++;
      this.column = 1;
    } else {
      this.column++;
    }
    return char;
  }

  private peek(offset = 0): string {
    return this.sql[this.pos + offset] || '';
  }

  private skipWhitespaceAndComments(): void {
    while (this.pos < this.sql.length) {
      const char = this.peek();

      // Whitespace
      if (/\s/.test(char)) {
        this.advance();
        continue;
      }

      // Single-line comment
      if (char === '-' && this.peek(1) === '-') {
        while (this.pos < this.sql.length && this.peek() !== '\n') {
          this.advance();
        }
        continue;
      }

      // Multi-line comment
      if (char === '/' && this.peek(1) === '*') {
        this.advance(); // /
        this.advance(); // *
        while (this.pos < this.sql.length - 1) {
          if (this.peek() === '*' && this.peek(1) === '/') {
            this.advance(); // *
            this.advance(); // /
            break;
          }
          this.advance();
        }
        continue;
      }

      break;
    }
  }

  private readToken(): void {
    const loc = this.location();
    const char = this.peek();

    // String literal
    if (char === "'" || char === '"') {
      this.readString(loc);
      return;
    }

    // Number
    if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(this.peek(1)))) {
      this.readNumber(loc);
      return;
    }

    // Parameter
    if (char === '$' || char === '?') {
      this.readParameter(loc);
      return;
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(char)) {
      this.readIdentifier(loc);
      return;
    }

    // Multi-char operators
    const twoChar = this.peek() + this.peek(1);
    if (['<=', '>=', '<>', '!=', '||'].includes(twoChar)) {
      this.advance();
      this.advance();
      this.tokens.push({ type: 'operator', value: twoChar, location: loc });
      return;
    }

    // Single char operators
    if ('=<>+-*/%'.includes(char)) {
      this.tokens.push({ type: 'operator', value: this.advance(), location: loc });
      return;
    }

    // Punctuation
    if ('(),;.'.includes(char)) {
      this.tokens.push({ type: 'punctuation', value: this.advance(), location: loc });
      return;
    }

    throw new SyntaxError(`Unexpected character '${char}'`, loc);
  }

  private readString(loc: SourceLocation): void {
    const quote = this.advance();
    let value = '';

    while (this.pos < this.sql.length) {
      const char = this.peek();

      if (char === quote) {
        if (this.peek(1) === quote) {
          // Escaped quote
          this.advance();
          value += this.advance();
        } else {
          this.advance();
          break;
        }
      } else if (char === '\\') {
        this.advance();
        const escaped = this.advance();
        switch (escaped) {
          case 'n': value += '\n'; break;
          case 't': value += '\t'; break;
          case 'r': value += '\r'; break;
          default: value += escaped;
        }
      } else {
        value += this.advance();
      }
    }

    this.tokens.push({ type: 'string', value, location: loc });
  }

  private readNumber(loc: SourceLocation): void {
    let value = '';

    while (/[0-9.]/.test(this.peek())) {
      value += this.advance();
    }

    // Scientific notation
    if (this.peek().toLowerCase() === 'e') {
      value += this.advance();
      if (this.peek() === '+' || this.peek() === '-') {
        value += this.advance();
      }
      while (/[0-9]/.test(this.peek())) {
        value += this.advance();
      }
    }

    this.tokens.push({ type: 'number', value, location: loc });
  }

  private readParameter(loc: SourceLocation): void {
    let value = this.advance();

    while (/[0-9]/.test(this.peek())) {
      value += this.advance();
    }

    this.tokens.push({ type: 'parameter', value, location: loc });
  }

  private readIdentifier(loc: SourceLocation): void {
    let value = '';

    while (/[a-zA-Z0-9_]/.test(this.peek())) {
      value += this.advance();
    }

    const lower = value.toLowerCase();
    if (KEYWORDS.has(lower)) {
      this.tokens.push({ type: 'keyword', value: lower, location: loc });
    } else {
      this.tokens.push({ type: 'identifier', value, location: loc });
    }
  }
}

// =============================================================================
// SYNTAX ERROR
// =============================================================================

export class SyntaxError extends Error {
  location?: SourceLocation;

  constructor(message: string, location?: SourceLocation) {
    const locStr = location ? ` at line ${location.line}, column ${location.column}` : '';
    super(message + locStr);
    this.name = 'SyntaxError';
    this.location = location;
  }
}

// =============================================================================
// SUBQUERY PARSER
// =============================================================================

export class SubqueryParser {
  private tokens: Token[] = [];
  private pos = 0;
  private outerScopes: Map<string, string>[] = []; // Track table aliases in outer scopes

  /**
   * Parse SQL string into AST with full subquery support
   */
  parse(sql: string): ParsedSelect {
    const tokenizer = new Tokenizer(sql);
    this.tokens = tokenizer.tokenize();
    this.pos = 0;
    this.outerScopes = [];

    // Handle WITH clause
    let cte: CTEDefinition[] | undefined;
    if (this.matchKeyword('with')) {
      cte = this.parseCTE();
    }

    const result = this.parseSelect();
    if (cte) {
      result.cte = cte;
    }

    if (!this.isAtEnd()) {
      throw new SyntaxError(`Unexpected token '${this.current().value}'`, this.current().location);
    }

    return result;
  }

  // ===========================================================================
  // TOKEN UTILITIES
  // ===========================================================================

  private current(): Token {
    return this.tokens[this.pos] || { type: 'eof', value: '', location: { line: 0, column: 0, offset: 0 } };
  }

  private peek(offset = 0): Token {
    return this.tokens[this.pos + offset] || { type: 'eof', value: '', location: { line: 0, column: 0, offset: 0 } };
  }

  private advance(): Token {
    return this.tokens[this.pos++] || { type: 'eof', value: '', location: { line: 0, column: 0, offset: 0 } };
  }

  private isAtEnd(): boolean {
    return this.current().type === 'eof';
  }

  private match(type: TokenType, value?: string): boolean {
    const token = this.current();
    return token.type === type && (value === undefined || token.value.toLowerCase() === value.toLowerCase());
  }

  private matchKeyword(...keywords: string[]): boolean {
    const token = this.current();
    return token.type === 'keyword' && keywords.includes(token.value.toLowerCase());
  }

  private expect(type: TokenType, value?: string): Token {
    const token = this.current();
    if (token.type !== type || (value !== undefined && token.value.toLowerCase() !== value.toLowerCase())) {
      throw new SyntaxError(
        `Expected ${type}${value ? ` '${value}'` : ''}, got ${token.type} '${token.value}'`,
        token.location
      );
    }
    return this.advance();
  }

  // ===========================================================================
  // CTE PARSING
  // ===========================================================================

  private parseCTE(): CTEDefinition[] {
    this.advance(); // WITH
    const ctes: CTEDefinition[] = [];

    const recursive = this.matchKeyword('recursive');
    if (recursive) this.advance();

    do {
      const loc = this.current().location;
      const name = this.expect('identifier').value;

      let columns: string[] | undefined;
      if (this.match('punctuation', '(')) {
        this.advance();
        columns = [];
        do {
          columns.push(this.expect('identifier').value);
        } while (this.match('punctuation', ',') && this.advance());
        this.expect('punctuation', ')');
      }

      this.expect('keyword', 'as');
      this.expect('punctuation', '(');

      // Push CTE name to outer scope so recursive references work
      this.outerScopes.push(new Map([[name, name]]));
      const query = this.parseSelect();
      this.outerScopes.pop();

      this.expect('punctuation', ')');

      ctes.push({ name, columns, query, recursive, location: loc });
    } while (this.match('punctuation', ',') && this.advance());

    return ctes;
  }

  // ===========================================================================
  // SELECT PARSING
  // ===========================================================================

  private parseSelect(): ParsedSelect {
    const loc = this.current().location;
    this.expect('keyword', 'select');

    const distinct = this.matchKeyword('distinct');
    if (distinct) this.advance();

    // Temporarily save position to look ahead for FROM clause
    const savedPos = this.pos;

    // Skip SELECT list to find FROM clause and register table in scope first
    // This enables correlation detection in SELECT subqueries
    let fromTable: string | undefined;
    let fromAlias: string | undefined;
    let parenDepth = 0;
    while (!this.isAtEnd()) {
      if (this.match('punctuation', '(')) {
        parenDepth++;
        this.advance();
      } else if (this.match('punctuation', ')')) {
        parenDepth--;
        this.advance();
      } else if (parenDepth === 0 && this.matchKeyword('from')) {
        this.advance();
        if (this.match('identifier')) {
          fromTable = this.advance().value;
          if (this.matchKeyword('as')) {
            this.advance();
            if (this.match('identifier')) {
              fromAlias = this.advance().value;
            }
          } else if (this.match('identifier') && !this.isClauseKeyword() && !this.isJoinKeyword()) {
            fromAlias = this.advance().value;
          }
        }
        break;
      } else {
        this.advance();
      }
    }

    // Restore position
    this.pos = savedPos;

    // Register the outer table in scope BEFORE parsing SELECT list
    const currentScope = new Map<string, string>();
    if (fromTable) {
      const tableRef = fromAlias || fromTable;
      currentScope.set(tableRef, tableRef);
      // Also register by table name if aliased
      if (fromAlias && fromTable !== fromAlias) {
        currentScope.set(fromTable, tableRef);
      }
    }
    this.outerScopes.push(currentScope);

    try {
      const columns = this.parseSelectList();

      this.expect('keyword', 'from');
      const from = this.parseFromClause();

      // Update scope with actual FROM info (handles derived tables)
      const tableAlias = from.type === 'table'
        ? from.alias || from.table
        : from.alias;
      if (tableAlias && !currentScope.has(tableAlias)) {
        currentScope.set(tableAlias, tableAlias);
      }

      const joins = this.parseJoins();
      for (const join of joins) {
        const alias = join.alias || (join.table.type === 'table' ? join.table.table : join.table.alias);
        if (alias) currentScope.set(alias, alias);
      }

      let where: ParsedExpr | undefined;
      if (this.matchKeyword('where')) {
        this.advance();
        where = this.parseExpression();
      }

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

      let having: ParsedExpr | undefined;
      if (this.matchKeyword('having')) {
        this.advance();
        having = this.parseExpression();
      }

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

      let limit: number | undefined;
      if (this.matchKeyword('limit')) {
        this.advance();
        limit = parseInt(this.expect('number').value, 10);
      }

      let offset: number | undefined;
      if (this.matchKeyword('offset')) {
        this.advance();
        offset = parseInt(this.expect('number').value, 10);
      }

      // UNION/INTERSECT/EXCEPT
      let union: SetOperation | undefined;
      if (this.matchKeyword('union', 'intersect', 'except')) {
        union = this.parseSetOperation();
      }

      return {
        type: 'select',
        columns,
        from,
        joins: joins.length > 0 ? joins : undefined,
        where,
        groupBy,
        having,
        orderBy,
        limit,
        offset,
        distinct,
        union,
        location: loc,
      };
    } finally {
      this.outerScopes.pop();
    }
  }

  // ===========================================================================
  // SELECT LIST PARSING
  // ===========================================================================

  private parseSelectList(): ParsedColumn[] {
    const columns: ParsedColumn[] = [];

    do {
      columns.push(this.parseSelectColumn());
    } while (this.match('punctuation', ',') && this.advance());

    return columns;
  }

  private parseSelectColumn(): ParsedColumn {
    const loc = this.current().location;

    // Handle *
    if (this.match('operator', '*')) {
      this.advance();
      return { expr: { type: 'star', location: loc }, location: loc };
    }

    const expr = this.parseExpression();

    let alias: string | undefined;
    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier') && !this.isClauseKeyword()) {
      alias = this.advance().value;
    }

    return { expr, alias, location: loc };
  }

  private isClauseKeyword(): boolean {
    return this.matchKeyword(
      'from', 'where', 'group', 'having', 'order', 'limit', 'offset',
      'union', 'intersect', 'except', 'join', 'inner', 'left', 'right', 'full', 'cross'
    );
  }

  // ===========================================================================
  // FROM CLAUSE PARSING
  // ===========================================================================

  private parseFromClause(): ParsedFrom {
    const loc = this.current().location;

    // Check for subquery (derived table)
    if (this.match('punctuation', '(')) {
      return this.parseDerivedTable(loc);
    }

    const table = this.expect('identifier').value;
    let alias: string | undefined;

    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier') && !this.isClauseKeyword() && !this.isJoinKeyword()) {
      alias = this.advance().value;
    }

    return { type: 'table', table, alias, location: loc };
  }

  private parseDerivedTable(loc: SourceLocation, lateral = false): DerivedTable {
    this.expect('punctuation', '(');

    if (!this.matchKeyword('select')) {
      throw new SyntaxError('Expected SELECT in derived table', this.current().location);
    }

    const query = this.parseSelect();
    this.expect('punctuation', ')');

    // Derived tables must have an alias
    let alias: string;
    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier')) {
      alias = this.advance().value;
    } else {
      throw new SyntaxError('Derived table requires an alias', this.current().location);
    }

    return { type: 'derived', query, alias, lateral, location: loc };
  }

  private isJoinKeyword(): boolean {
    return this.matchKeyword('join', 'inner', 'left', 'right', 'full', 'cross', 'natural', 'lateral');
  }

  // ===========================================================================
  // JOIN PARSING
  // ===========================================================================

  private parseJoins(): ParsedJoin[] {
    const joins: ParsedJoin[] = [];

    while (this.isJoinKeyword()) {
      joins.push(this.parseJoin());
    }

    return joins;
  }

  private parseJoin(): ParsedJoin {
    const loc = this.current().location;
    let type: ParsedJoin['type'] = 'inner';
    let lateral = false;

    if (this.matchKeyword('natural')) {
      this.advance();
    }

    // LATERAL can appear before or after the join type
    if (this.matchKeyword('lateral')) {
      lateral = true;
      this.advance();
    }

    if (this.matchKeyword('cross')) {
      type = 'cross';
      this.advance();
    } else if (this.matchKeyword('left')) {
      type = 'left';
      this.advance();
      this.matchKeyword('outer') && this.advance();
    } else if (this.matchKeyword('right')) {
      type = 'right';
      this.advance();
      this.matchKeyword('outer') && this.advance();
    } else if (this.matchKeyword('full')) {
      type = 'full';
      this.advance();
      this.matchKeyword('outer') && this.advance();
    } else if (this.matchKeyword('inner')) {
      this.advance();
    }

    // LATERAL keyword can also appear after join type, before JOIN keyword
    if (this.matchKeyword('lateral')) {
      lateral = true;
      this.advance();
    }

    this.expect('keyword', 'join');

    // LATERAL can also appear AFTER the JOIN keyword (e.g., CROSS JOIN LATERAL)
    if (this.matchKeyword('lateral')) {
      lateral = true;
      this.advance();
    }

    let table: ParsedFrom;
    if (this.match('punctuation', '(')) {
      table = this.parseDerivedTable(loc, lateral);
    } else {
      const tableName = this.expect('identifier').value;
      let alias: string | undefined;

      if (this.matchKeyword('as')) {
        this.advance();
        alias = this.expect('identifier').value;
      } else if (this.match('identifier') && !this.matchKeyword('on', 'using') && !this.isClauseKeyword()) {
        alias = this.advance().value;
      }

      table = { type: 'table', table: tableName, alias, location: loc };
    }

    let on: ParsedExpr | undefined;
    if (this.matchKeyword('on')) {
      this.advance();
      on = this.parseExpression();
    }

    return { type, table, on, lateral, location: loc };
  }

  // ===========================================================================
  // SET OPERATIONS
  // ===========================================================================

  private parseSetOperation(): SetOperation {
    const loc = this.current().location;
    const type = this.advance().value as 'union' | 'intersect' | 'except';

    const all = this.matchKeyword('all');
    if (all) this.advance();

    const query = this.parseSelect();

    return { type, all, query, location: loc };
  }

  // ===========================================================================
  // ORDER BY PARSING
  // ===========================================================================

  private parseOrderByItem(): ParsedOrderBy {
    const loc = this.current().location;
    const expr = this.parseExpression();

    let direction: 'asc' | 'desc' = 'asc';
    if (this.matchKeyword('asc')) {
      this.advance();
    } else if (this.matchKeyword('desc')) {
      direction = 'desc';
      this.advance();
    }

    let nullsFirst: boolean | undefined;
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

    return { expr, direction, nullsFirst, location: loc };
  }

  // ===========================================================================
  // EXPRESSION PARSING
  // ===========================================================================

  private parseExpression(): ParsedExpr {
    return this.parseOr();
  }

  private parseOr(): ParsedExpr {
    let left = this.parseAnd();

    while (this.matchKeyword('or')) {
      const loc = this.current().location;
      this.advance();
      const right = this.parseAnd();
      left = { type: 'binary', op: 'or', left, right, location: loc };
    }

    return left;
  }

  private parseAnd(): ParsedExpr {
    let left = this.parseNot();

    while (this.matchKeyword('and')) {
      const loc = this.current().location;
      this.advance();
      const right = this.parseNot();
      left = { type: 'binary', op: 'and', left, right, location: loc };
    }

    return left;
  }

  private parseNot(): ParsedExpr {
    if (this.matchKeyword('not')) {
      const loc = this.current().location;
      this.advance();
      return { type: 'unary', op: 'not', operand: this.parseNot(), location: loc };
    }
    return this.parseComparison();
  }

  private parseComparison(): ParsedExpr {
    const loc = this.current().location;
    let left = this.parseAddSub();

    // EXISTS
    if (left.type === 'exists') {
      return left;
    }

    // IS NULL / IS NOT NULL
    if (this.matchKeyword('is')) {
      this.advance();
      const isNot = this.matchKeyword('not');
      if (isNot) this.advance();
      this.expect('keyword', 'null');
      return { type: 'isNull', expr: left, isNot, location: loc };
    }

    // BETWEEN
    if (this.matchKeyword('between')) {
      this.advance();
      const low = this.parseAddSub();
      this.expect('keyword', 'and');
      const high = this.parseAddSub();
      return { type: 'between', expr: left, low, high, location: loc };
    }

    // IN / NOT IN
    if (this.matchKeyword('in') || (this.matchKeyword('not') && this.peek(1).value === 'in')) {
      const not = this.matchKeyword('not');
      if (not) this.advance();
      this.advance(); // IN

      return this.parseInExpression(left, not, loc);
    }

    // NOT BETWEEN
    if (this.matchKeyword('not') && this.peek(1).value === 'between') {
      this.advance(); // NOT
      this.advance(); // BETWEEN
      const low = this.parseAddSub();
      this.expect('keyword', 'and');
      const high = this.parseAddSub();
      return { type: 'unary', op: 'not', operand: { type: 'between', expr: left, low, high, location: loc }, location: loc };
    }

    // Comparison with ANY/ALL/SOME
    const compOps: Record<string, ComparisonOp> = {
      '=': 'eq', '<>': 'ne', '!=': 'ne', '<': 'lt', '<=': 'le', '>': 'gt', '>=': 'ge',
    };

    if (this.match('operator') && compOps[this.current().value]) {
      const op = compOps[this.advance().value];

      // Check for ANY/ALL/SOME
      if (this.matchKeyword('any', 'all', 'some')) {
        const quantifier = this.advance().value as 'any' | 'all' | 'some';
        this.expect('punctuation', '(');
        const subquery = this.parseSubquery('any');
        this.expect('punctuation', ')');

        return {
          type: 'comparison',
          op,
          quantifier,
          left,
          right: subquery,
          location: loc,
        };
      }

      const right = this.parseAddSub();
      return { type: 'binary', op, left, right, location: loc };
    }

    // LIKE / ILIKE
    if (this.matchKeyword('like', 'ilike')) {
      const op = this.advance().value;
      const right = this.parseAddSub();
      return { type: 'binary', op, left, right, location: loc };
    }

    return left;
  }

  private parseInExpression(left: ParsedExpr, not: boolean, loc: SourceLocation): ParsedExpr {
    this.expect('punctuation', '(');

    // Check if it's a subquery
    if (this.matchKeyword('select')) {
      const subquery = this.parseSubquery('in');
      this.expect('punctuation', ')');

      const inExpr: ParsedExpr = { type: 'in', expr: left, values: subquery, location: loc };
      return not
        ? { type: 'unary', op: 'not', operand: inExpr, location: loc }
        : inExpr;
    }

    // It's a value list - must have at least one value
    if (this.match('punctuation', ')')) {
      throw new SyntaxError('Expected at least one value in IN list', this.current().location);
    }

    const values: ParsedExpr[] = [];
    values.push(this.parseExpression());
    while (this.match('punctuation', ',')) {
      this.advance();
      values.push(this.parseExpression());
    }

    this.expect('punctuation', ')');

    const inExpr: ParsedExpr = { type: 'in', expr: left, values, location: loc };
    return not
      ? { type: 'unary', op: 'not', operand: inExpr, location: loc }
      : inExpr;
  }

  private parseAddSub(): ParsedExpr {
    let left = this.parseMulDiv();

    while (this.match('operator', '+') || this.match('operator', '-')) {
      const loc = this.current().location;
      const op = this.advance().value === '+' ? 'add' : 'sub';
      const right = this.parseMulDiv();
      left = { type: 'binary', op, left, right, location: loc };
    }

    return left;
  }

  private parseMulDiv(): ParsedExpr {
    let left = this.parseUnary();

    while (this.match('operator', '*') || this.match('operator', '/') || this.match('operator', '%')) {
      const loc = this.current().location;
      const opChar = this.advance().value;
      const op = opChar === '*' ? 'mul' : opChar === '/' ? 'div' : 'mod';
      const right = this.parseUnary();
      left = { type: 'binary', op, left, right, location: loc };
    }

    return left;
  }

  private parseUnary(): ParsedExpr {
    if (this.match('operator', '-')) {
      const loc = this.current().location;
      this.advance();
      return { type: 'unary', op: 'neg', operand: this.parseUnary(), location: loc };
    }
    return this.parsePrimary();
  }

  private parsePrimary(): ParsedExpr {
    const loc = this.current().location;

    // Parenthesized expression or subquery
    if (this.match('punctuation', '(')) {
      this.advance();

      // Check if it's a subquery
      if (this.matchKeyword('select')) {
        const subquery = this.parseSubquery('scalar');
        this.expect('punctuation', ')');
        return subquery;
      }

      // Check for tuple (multi-element in parentheses)
      const expr = this.parseExpression();
      if (this.match('punctuation', ',')) {
        const elements: ParsedExpr[] = [expr];
        while (this.match('punctuation', ',')) {
          this.advance();
          elements.push(this.parseExpression());
        }
        this.expect('punctuation', ')');
        return { type: 'tuple', elements, location: loc };
      }

      this.expect('punctuation', ')');
      return expr;
    }

    // EXISTS
    if (this.matchKeyword('exists')) {
      this.advance();
      this.expect('punctuation', '(');
      const query = this.parseSelect();
      this.expect('punctuation', ')');

      const correlatedColumns = this.findCorrelatedColumns(query);

      return {
        type: 'exists',
        query,
        subqueryType: 'exists',
        correlatedColumns,
        isCorrelated: correlatedColumns.length > 0,
        canBeSemiJoin: this.canConvertToSemiJoin(query, correlatedColumns),
        location: loc,
      };
    }

    // CASE expression
    if (this.matchKeyword('case')) {
      return this.parseCaseExpression(loc);
    }

    // Aggregate functions
    if (this.matchKeyword('count', 'sum', 'avg', 'min', 'max')) {
      return this.parseAggregate(loc);
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
        return { type: 'function', name, args, location: loc };
      }

      // Table.column
      if (this.match('punctuation', '.')) {
        this.advance();

        // table.*
        if (this.match('operator', '*')) {
          this.advance();
          return { type: 'star', location: loc };
        }

        const column = this.expect('identifier').value;
        return { type: 'column', name: column, table: name, location: loc };
      }

      return { type: 'column', name, location: loc };
    }

    // Number
    if (this.match('number')) {
      const value = this.advance().value;
      const numValue = value.includes('.') ? parseFloat(value) : parseInt(value, 10);
      return { type: 'literal', value: numValue, location: loc };
    }

    // String
    if (this.match('string')) {
      return { type: 'literal', value: this.advance().value, location: loc };
    }

    // Boolean literals
    if (this.matchKeyword('true')) {
      this.advance();
      return { type: 'literal', value: true, location: loc };
    }
    if (this.matchKeyword('false')) {
      this.advance();
      return { type: 'literal', value: false, location: loc };
    }

    // NULL
    if (this.matchKeyword('null')) {
      this.advance();
      return { type: 'literal', value: null, location: loc };
    }

    // Parameter
    if (this.match('parameter')) {
      const param = this.advance().value;
      return { type: 'column', name: param, location: loc };
    }

    // Star
    if (this.match('operator', '*')) {
      this.advance();
      return { type: 'star', location: loc };
    }

    throw new SyntaxError(`Unexpected token '${this.current().value}'`, this.current().location);
  }

  // ===========================================================================
  // SUBQUERY PARSING
  // ===========================================================================

  private parseSubquery(subqueryType: SubqueryNode['subqueryType']): SubqueryNode {
    const loc = this.current().location;
    const query = this.parseSelect();

    // Validate scalar subquery returns single column
    if (subqueryType === 'scalar' && query.columns.length > 1) {
      const nonStar = query.columns.filter(c => c.expr.type !== 'star');
      if (nonStar.length > 1) {
        throw new SyntaxError(
          'Scalar subquery must return a single column',
          loc
        );
      }
    }

    const correlatedColumns = this.findCorrelatedColumns(query);

    return {
      type: 'subquery',
      subqueryType,
      query,
      correlatedColumns,
      isCorrelated: correlatedColumns.length > 0,
      canBeSemiJoin: subqueryType === 'in' && correlatedColumns.length === 0,
      canBeAntiJoin: subqueryType === 'in' && correlatedColumns.length === 0,
      location: loc,
    };
  }

  // ===========================================================================
  // CASE EXPRESSION
  // ===========================================================================

  private parseCaseExpression(loc: SourceLocation): ParsedExpr {
    this.advance(); // CASE

    let operand: ParsedExpr | undefined;
    if (!this.matchKeyword('when')) {
      operand = this.parseExpression();
    }

    const whens: { condition: ParsedExpr; result: ParsedExpr }[] = [];
    while (this.matchKeyword('when')) {
      this.advance();
      const condition = this.parseExpression();
      this.expect('keyword', 'then');
      const result = this.parseExpression();
      whens.push({ condition, result });
    }

    let else_: ParsedExpr | undefined;
    if (this.matchKeyword('else')) {
      this.advance();
      else_ = this.parseExpression();
    }

    this.expect('keyword', 'end');

    return { type: 'case', operand, whens, else_, location: loc };
  }

  // ===========================================================================
  // AGGREGATE PARSING
  // ===========================================================================

  private parseAggregate(loc: SourceLocation): ParsedExpr {
    const name = this.advance().value;
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

    return { type: 'aggregate', name, arg, distinct, location: loc };
  }

  // ===========================================================================
  // CORRELATION DETECTION
  // ===========================================================================

  private findCorrelatedColumns(query: ParsedSelect): CorrelatedColumn[] {
    const correlated: CorrelatedColumn[] = [];
    const innerTables = this.collectInnerTables(query);

    this.walkExpressions(query, (expr) => {
      if (expr.type === 'column' && expr.table) {
        // Check if this table reference is from outer scope
        if (!innerTables.has(expr.table) && this.isOuterReference(expr.table)) {
          correlated.push({ table: expr.table, column: expr.name });
        }
      }
    });

    return correlated;
  }

  private collectInnerTables(query: ParsedSelect): Set<string> {
    const tables = new Set<string>();

    if (query.from.type === 'table') {
      tables.add(query.from.alias || query.from.table);
    } else {
      tables.add(query.from.alias);
    }

    if (query.joins) {
      for (const join of query.joins) {
        if (join.table.type === 'table') {
          tables.add(join.alias || join.table.alias || join.table.table);
        } else {
          tables.add(join.table.alias);
        }
      }
    }

    return tables;
  }

  private isOuterReference(table: string): boolean {
    for (const scope of this.outerScopes) {
      if (scope.has(table)) return true;
    }
    return false;
  }

  private walkExpressions(query: ParsedSelect, callback: (expr: ParsedExpr) => void): void {
    // Walk columns
    for (const col of query.columns) {
      this.walkExpression(col.expr, callback);
    }

    // Walk WHERE
    if (query.where) {
      this.walkExpression(query.where, callback);
    }

    // Walk HAVING
    if (query.having) {
      this.walkExpression(query.having, callback);
    }

    // Walk JOINs
    if (query.joins) {
      for (const join of query.joins) {
        if (join.on) {
          this.walkExpression(join.on, callback);
        }
      }
    }
  }

  private walkExpression(expr: ParsedExpr, callback: (expr: ParsedExpr) => void): void {
    callback(expr);

    switch (expr.type) {
      case 'binary':
        this.walkExpression(expr.left, callback);
        this.walkExpression(expr.right, callback);
        break;
      case 'unary':
        this.walkExpression(expr.operand, callback);
        break;
      case 'function':
        for (const arg of expr.args) {
          this.walkExpression(arg, callback);
        }
        break;
      case 'aggregate':
        if (expr.arg !== '*') {
          this.walkExpression(expr.arg, callback);
        }
        break;
      case 'between':
        this.walkExpression(expr.expr, callback);
        this.walkExpression(expr.low, callback);
        this.walkExpression(expr.high, callback);
        break;
      case 'in':
        this.walkExpression(expr.expr, callback);
        if (Array.isArray(expr.values)) {
          for (const val of expr.values) {
            this.walkExpression(val, callback);
          }
        }
        break;
      case 'isNull':
        this.walkExpression(expr.expr, callback);
        break;
      case 'case':
        if (expr.operand) this.walkExpression(expr.operand, callback);
        for (const when of expr.whens) {
          this.walkExpression(when.condition, callback);
          this.walkExpression(when.result, callback);
        }
        if (expr.else_) this.walkExpression(expr.else_, callback);
        break;
      case 'tuple':
        for (const el of expr.elements) {
          this.walkExpression(el, callback);
        }
        break;
      case 'subquery':
        this.walkExpressions(expr.query, callback);
        break;
      case 'exists':
        this.walkExpressions(expr.query, callback);
        break;
      case 'comparison':
        this.walkExpression(expr.left, callback);
        if (expr.right.type === 'subquery') {
          this.walkExpressions(expr.right.query, callback);
        }
        break;
    }
  }

  private canConvertToSemiJoin(query: ParsedSelect, correlatedColumns: CorrelatedColumn[]): boolean {
    // A subquery can be converted to semi-join if:
    // 1. It references outer columns in a simple equality
    // 2. No aggregation or DISTINCT
    // 3. No LIMIT/OFFSET

    if (query.distinct || query.limit || query.offset) return false;
    if (query.groupBy || query.having) return false;

    // Check if correlated predicate is simple equality
    if (correlatedColumns.length > 0 && query.where) {
      return this.hasSimpleCorrelatedEquality(query.where, correlatedColumns);
    }

    return true;
  }

  private hasSimpleCorrelatedEquality(expr: ParsedExpr, correlatedColumns: CorrelatedColumn[]): boolean {
    if (expr.type === 'binary' && expr.op === 'eq') {
      const leftCol = expr.left.type === 'column' ? expr.left : null;
      const rightCol = expr.right.type === 'column' ? expr.right : null;

      const leftIsCorrelated = leftCol && correlatedColumns.some(
        c => c.table === leftCol.table && c.column === leftCol.name
      );
      const rightIsCorrelated = rightCol && correlatedColumns.some(
        c => c.table === rightCol.table && c.column === rightCol.name
      );

      return leftIsCorrelated !== rightIsCorrelated; // XOR - one side is correlated
    }

    if (expr.type === 'binary' && expr.op === 'and') {
      return this.hasSimpleCorrelatedEquality(expr.left, correlatedColumns) ||
             this.hasSimpleCorrelatedEquality(expr.right, correlatedColumns);
    }

    return false;
  }
}

// =============================================================================
// CONVENIENCE FUNCTION
// =============================================================================

/**
 * Parse SQL string with full subquery support
 */
export function parseSubquery(sql: string): ParsedSelect {
  return new SubqueryParser().parse(sql);
}
